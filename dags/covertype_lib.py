"""Logica del pipeline: recoleccion, procesamiento, preparacion y entrenamiento."""
import io
import json
import os
from datetime import datetime

import boto3
import joblib
import pandas as pd
import requests
import sqlalchemy as sa
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

COLUMNAS = [
    "elevation", "aspect", "slope",
    "horizontal_distance_to_hydrology", "vertical_distance_to_hydrology",
    "horizontal_distance_to_roadways",
    "hillshade_9am", "hillshade_noon", "hillshade_3pm",
    "horizontal_distance_to_fire_points",
    "wilderness_area", "soil_type", "cover_type",
]

NUMERICAS = COLUMNAS[:10]
CATEGORICAS = ["wilderness_area", "soil_type"]
OBJETIVO = "cover_type"


def _engine():
    return sa.create_engine(os.environ["DATA_DB_CONN"])


def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def recolectar(**context):
    """UNA sola peticion a la API externa por ejecucion del DAG."""
    url = f"{os.environ['DATA_API_URL']}/data"
    grupo = os.environ["GROUP_NUMBER"]
    resp = requests.get(url, params={"group_number": grupo}, timeout=60)

    if resp.status_code == 400:
        print(f"La API respondio 400: {resp.text}")
        print("Posiblemente ya se alcanzo la recoleccion minima del grupo.")
        return {"batch_number": None, "filas": 0}

    resp.raise_for_status()
    payload = resp.json()
    batch = payload["batch_number"]
    filas = payload["data"]

    df = pd.DataFrame(filas, columns=COLUMNAS)
    df["batch_number"] = batch
    df["dag_run_id"] = context["run_id"]

    df.to_sql("covertype_raw", _engine(), schema="raw",
              if_exists="append", index=False, chunksize=5000, method="multi")

    print(f"Batch {batch}: {len(df)} filas insertadas en raw")
    return {"batch_number": batch, "filas": len(df)}


def procesar(**context):
    """Reconstruye clean a partir de TODO lo acumulado en raw."""
    engine = _engine()
    df = pd.read_sql(f"SELECT {', '.join(COLUMNAS)} FROM raw.covertype_raw", engine)
    inicial = len(df)

    for col in NUMERICAS + [OBJETIVO]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in CATEGORICAS:
        df[col] = df[col].astype(str).str.strip()
        df.loc[df[col].isin(["", "nan", "None", "NA"]), col] = pd.NA

    df = df.dropna()
    sin_nulos = len(df)

    df = df[(df["aspect"].between(0, 360)) & (df["slope"].between(0, 90))]
    df = df[df["hillshade_9am"].between(0, 255)]
    df = df[df["hillshade_noon"].between(0, 255)]
    df = df[df["hillshade_3pm"].between(0, 255)]
    en_rango = len(df)

    df = df.drop_duplicates()
    final = len(df)

    for col in NUMERICAS + [OBJETIVO]:
        df[col] = df[col].astype(int)

    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE clean.covertype_clean RESTART IDENTITY"))
    df.to_sql("covertype_clean", engine, schema="clean",
              if_exists="append", index=False, chunksize=5000, method="multi")

    print(f"raw: {inicial} -> sin nulos: {sin_nulos} -> en rango: {en_rango} -> sin duplicados: {final}")
    return {"filas_clean": final}


def preparar(**context):
    """Reconstruye train marcando particion estratificada 80/20."""
    engine = _engine()
    df = pd.read_sql(f"SELECT {', '.join(COLUMNAS)} FROM clean.covertype_clean", engine)

    conteo = df[OBJETIVO].value_counts()
    print(f"Distribucion de clases:\n{conteo.sort_index()}")

    estratos = df[OBJETIVO] if (conteo >= 2).all() else None
    if estratos is None:
        print("Alguna clase tiene menos de 2 filas: particion sin estratificar")

    idx_train, idx_test = train_test_split(
        df.index, test_size=0.2, random_state=42, stratify=estratos
    )
    df["split"] = "train"
    df.loc[idx_test, "split"] = "test"

    with engine.begin() as conn:
        conn.execute(sa.text("TRUNCATE TABLE train.covertype_train RESTART IDENTITY"))
    df.to_sql("covertype_train", engine, schema="train",
              if_exists="append", index=False, chunksize=5000, method="multi")

    print(f"train: {(df['split'] == 'train').sum()} | test: {(df['split'] == 'test').sum()}")
    return {"train": int((df["split"] == "train").sum()),
            "test": int((df["split"] == "test").sum())}


def entrenar(**context):
    """Entrena con train, evalua con test y sube el modelo a MinIO."""
    engine = _engine()
    cols = ", ".join(COLUMNAS + ["split"])
    df = pd.read_sql(f"SELECT {cols} FROM train.covertype_train", engine)

    tr = df[df["split"] == "train"]
    te = df[df["split"] == "test"]

    X_tr, y_tr = tr[NUMERICAS + CATEGORICAS], tr[OBJETIVO]
    X_te, y_te = te[NUMERICAS + CATEGORICAS], te[OBJETIVO]

    modelo = Pipeline([
        ("prep", ColumnTransformer([
            ("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAS),
        ], remainder="passthrough")),
        ("clf", RandomForestClassifier(
            n_estimators=100, max_depth=20, random_state=42, n_jobs=-1)),
    ])
    modelo.fit(X_tr, y_tr)

    pred = modelo.predict(X_te)
    metricas = {
        "accuracy": round(float(accuracy_score(y_te, pred)), 4),
        "f1_macro": round(float(f1_score(y_te, pred, average="macro")), 4),
    }
    print(f"Metricas: {metricas}")

    meta = {
        "entrenado_en": datetime.utcnow().isoformat(),
        "dag_run_id": context["run_id"],
        "filas_train": len(tr),
        "filas_test": len(te),
        "clases": sorted(int(c) for c in df[OBJETIVO].unique()),
        "metricas": metricas,
        "features": NUMERICAS + CATEGORICAS,
    }

    buffer = io.BytesIO()
    joblib.dump(modelo, buffer)
    buffer.seek(0)

    s3 = _s3()
    bucket = os.environ["MINIO_BUCKET"]
    sello = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    s3.put_object(Bucket=bucket, Key=f"history/modelo_{sello}.joblib",
                  Body=buffer.getvalue())
    s3.put_object(Bucket=bucket, Key="modelo_actual.joblib",
                  Body=buffer.getvalue())
    s3.put_object(Bucket=bucket, Key="modelo_actual.json",
                  Body=json.dumps(meta, indent=2).encode())

    print(f"Modelo subido a MinIO: modelo_actual.joblib e history/modelo_{sello}.joblib")
    return metricas
