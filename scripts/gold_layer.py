from pathlib import Path

import pandas as pd

def run_gold_layer( silver_file: str, **context) -> str:

    df = pd.read_csv(silver_file)

    agg = (
        df.groupby('origin_country')
        .agg(
            total_flights=('icao24', 'count'),
            avg_velocity=('velocity', 'mean'),
            on_ground=('on_ground', 'sum')
        ).reset_index()
    )

    gold_path = Path(silver_file.replace('silver', 'gold'))

    gold_path.parent.mkdir(parents=True, exist_ok=True)

    agg.to_csv(gold_path, index=False)

    return str(gold_path)