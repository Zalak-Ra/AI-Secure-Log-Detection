from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.layers import Dense, Input, LSTM, RepeatVector, TimeDistributed
from tensorflow.keras.models import Sequential


TIME_STEPS = 10
RESAMPLE_RULE = '1min'
DATA_PATH = Path(__file__).with_name('live_data.csv')

EXPECTED_COLS = [
    'timestamp',
    'machine',
    'cpu_usage',
    'gpu_wrk_util',
    'avg_mem',
    'max_mem',
    'avg_gpu_wrk_mem',
    'max_gpu_wrk_mem',
    'read',
    'write',
    'read_count',
    'write_count',
]

FEATURES = [
    'cpu_usage',
    'gpu_wrk_util',
    'avg_mem',
    'max_mem',
    'avg_gpu_wrk_mem',
    'max_gpu_wrk_mem',
    'read',
    'write',
    'read_count',
    'write_count',
]


def load_and_preprocess_data(data_path: Path) -> tuple[pd.DataFrame, np.ndarray]:
    df = pd.read_csv(data_path)
    if list(df.columns) != EXPECTED_COLS:
        if len(df.columns) == len(EXPECTED_COLS):
            df.columns = EXPECTED_COLS
        else:
            raise ValueError(
                f'Expected {len(EXPECTED_COLS)} columns, found {len(df.columns)}.'
            )

    numeric_ts = pd.to_numeric(df['timestamp'], errors='coerce')
    epoch_ts = pd.to_datetime(numeric_ts, unit='s', errors='coerce')
    legacy_ts = pd.to_datetime(
        df['timestamp'],
        format='%d-%m-%y %H:%M:%S',
        errors='coerce',
    )
    df['timestamp'] = epoch_ts.fillna(legacy_ts)

    original_count = len(df)
    df = df.dropna(subset=['timestamp', 'machine']).copy()

    df = df[df['timestamp'].dt.year >= 2025].copy()

    for col in FEATURES:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.dropna(subset=FEATURES).sort_values(['machine', 'timestamp']).copy()

    unique_servers = df['machine'].dropna().unique()
    print(
        f'Loaded {original_count} rows, kept {len(df)} clean rows (year >= 2025).'
    )
    print(f'Unique servers: {unique_servers}')

    return df, unique_servers


def build_sequences(
    df: pd.DataFrame,
    unique_servers: np.ndarray,
) -> tuple[list[np.ndarray], list[dict[str, object]], dict[str, MinMaxScaler]]:
    all_sequences: list[np.ndarray] = []
    sequence_metadata: list[dict[str, object]] = []
    scalers_by_server: dict[str, MinMaxScaler] = {}

    for mac in unique_servers:
        server_data = df[df['machine'] == mac].copy()
        min_time = server_data['timestamp'].min()
        max_time = server_data['timestamp'].max()
        time_gap = max_time - min_time

        print(f'\n--- {mac} ---')
        print(f'  First log : {min_time}')
        print(f'  Last log  : {max_time}')
        print(f'  Time span : {time_gap}')

        if time_gap.days > 2:
            print(f'  WARNING: Massive time gap detected for {mac}. Skipping this server.')
            continue

        server_minute = (
            server_data
            .set_index('timestamp')
            .resample(RESAMPLE_RULE)
            .mean(numeric_only=True)
            .dropna()
        )
        print(f'  Resampled to {len(server_minute)} clean 1-minute rows.')

        if len(server_minute) <= TIME_STEPS:
            print(
                f'  Not enough data for {mac} to create a '
                f'{TIME_STEPS}-step window. Skipping.'
            )
            continue

        scaler = MinMaxScaler()
        data_scaled = scaler.fit_transform(server_minute[FEATURES].values)
        scalers_by_server[mac] = scaler

        for i in range(len(data_scaled) - TIME_STEPS):
            all_sequences.append(data_scaled[i : i + TIME_STEPS])
            sequence_metadata.append(
                {
                    'machine': mac,
                    'start': server_minute.index[i],
                    'end': server_minute.index[i + TIME_STEPS - 1],
                }
            )

    print(f'\nTotal sequences built: {len(all_sequences)}')
    return all_sequences, sequence_metadata, scalers_by_server


def build_model(n_timesteps: int, n_features: int) -> Sequential:
    model = Sequential(
        [
            Input(shape=(n_timesteps, n_features)),
            LSTM(16, activation='relu', return_sequences=False),
            RepeatVector(n_timesteps),
            LSTM(16, activation='relu', return_sequences=True),
            TimeDistributed(Dense(n_features)),
        ]
    )
    model.compile(optimizer='adam', loss='mse')
    return model


def run_sanity_check(
    model: Sequential,
    x_train: np.ndarray,
    x_test: np.ndarray,
    threshold: float,
) -> None:
    failure_scenarios = {
        'memory_leak_oom': {'avg_mem': 1.4, 'max_mem': 1.6},
        'cpu_runaway_deadlock': {
            'cpu_usage': 1.6,
            'read': 0.0,
            'write': 0.0,
            'read_count': 0.0,
            'write_count': 0.0,
        },
        'gpu_crash_cuda_oom': {
            'gpu_wrk_util': 0.0,
            'avg_gpu_wrk_mem': 1.6,
            'max_gpu_wrk_mem': 1.6,
        },
        'network_ddos': {
            'read': 1.7,
            'read_count': 1.7,
            'cpu_usage': 1.4,
            'avg_mem': 1.3,
            'max_mem': 1.4,
        },
        'silent_death': {feature: 0.0 for feature in FEATURES},
    }

    feature_index = {feature: idx for idx, feature in enumerate(FEATURES)}
    base_windows = x_test if len(x_test) else x_train

    print('\n=== Injected Failure Sanity Check ===')
    for name, overrides in failure_scenarios.items():
        synthetic = base_windows.copy()
        for feature, value in overrides.items():
            synthetic[:, :, feature_index[feature]] = value

        synthetic_pred = model.predict(synthetic, verbose=0)
        synthetic_mse = np.mean(np.power(synthetic - synthetic_pred, 2), axis=(1, 2))
        detected = synthetic_mse > threshold
        print(f'{name}: {detected.sum()} / {len(detected)} windows flagged')


def main() -> None:
    print('All imports successful.')

    df, unique_servers = load_and_preprocess_data(DATA_PATH)
    all_sequences, sequence_metadata, scalers_by_server = build_sequences(
        df,
        unique_servers,
    )
    _ = scalers_by_server

    if not all_sequences:
        raise ValueError(
            'No sequences were built. Collect more data or verify timestamps are valid.'
        )

    x = np.array(all_sequences)
    print(f'Dataset shape: {x.shape}')

    x_train, x_test, train_meta, test_meta = train_test_split(
        x,
        sequence_metadata,
        test_size=0.2,
        random_state=42,
        shuffle=False,
    )
    print(f'Training samples : {x_train.shape[0]}')
    print(f'Testing  samples : {x_test.shape[0]}')

    n_timesteps = x_train.shape[1]
    n_features = x_train.shape[2]

    model = build_model(n_timesteps, n_features)
    model.summary()

    model.fit(
        x_train,
        x_train,
        epochs=20,
        batch_size=32,
        validation_split=0.1,
        shuffle=True,
        verbose=1,
    )

    x_train_pred = model.predict(x_train)
    train_mse = np.mean(np.power(x_train - x_train_pred, 2), axis=(1, 2))
    threshold = np.percentile(train_mse, 99)

    x_test_pred = model.predict(x_test)
    test_mse = np.mean(np.power(x_test - x_test_pred, 2), axis=(1, 2))
    anomalies = test_mse > threshold

    print('\n=== Anomaly Detection Results ===')
    print(f'Training Reconstruction Error - Mean : {np.mean(train_mse):.6f}')
    print(f'Training Reconstruction Error - P99  : {threshold:.6f}')
    print(f'Test Reconstruction Error     - Mean : {np.mean(test_mse):.6f}')
    print(
        f'Anomalies Detected                  : '
        f'{anomalies.sum()} / {len(test_mse)} sequences'
    )

    anomaly_indices = np.where(anomalies)[0]
    if len(anomaly_indices) > 0:
        print('\nFlagged sequences:')
        for idx in anomaly_indices[:25]:
            meta = test_meta[idx]
            print(
                f"  {meta['machine']} {meta['start']} -> {meta['end']} | "
                f'MSE = {test_mse[idx]:.6f} [ANOMALY]'
            )
    else:
        print('\nNo anomalies detected in the held-out test set.')

    _ = train_meta
    run_sanity_check(model, x_train, x_test, threshold)


if __name__ == '__main__':
    main()
