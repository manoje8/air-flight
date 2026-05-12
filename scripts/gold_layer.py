from pathlib import Path

import pandas as pd

def run_gold_layer(silver_file: str = None, **context) -> str:

    ti = context.get("ti")
    if not silver_file and ti:
        silver_file = ti.xcom_pull(key="silver_file")

    if not silver_file:
        raise ValueError("Silver file path not found")

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

    if ti:
        ti.xcom_push(key="gold_file", value=str(gold_path))

    return str(gold_path)
