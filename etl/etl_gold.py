import sys
import os

sys.path.append(os.path.abspath("./.secrets"))

import pandas as pd
import numpy as np
import datetime
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder

from psycopg2.extras import execute_values
from db_config import get_connection


# =============================
# LOG
# =============================
log_file = "etl.log"

def escribir_log(msg):
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} - {msg}\n")


def log_db(conn, pipeline, msg):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO meta.pipeline_log(pipeline_name, mensaje)
            VALUES (%s,%s)
        """, (pipeline, msg))
        conn.commit()
    except Exception as e:
        print("Error log_db:", e)


# =============================
# FEATURE ENGINEERING
# =============================
def feature_engineering(df):

    df["hora"] = pd.to_datetime(df["hora"], errors="coerce").dt.hour.fillna(0).astype(int)

    df["es_fin_semana"] = df["dia_semana"].isin(["Saturday", "Sunday"]).astype(int)

    df["hora_pico"] = df["hora"].apply(lambda x: 1 if (12 <= x <= 14 or 18 <= x <= 21) else 0)

    df["producto_promocion"] = df["producto"].astype(str) + "_" + df["tipo_promocion"].astype(str)

    df["temporada"] = pd.cut(df["mes"], bins=[0, 3, 6, 9, 12], labels=["Q1", "Q2", "Q3", "Q4"]).astype(str)

    return df


# =============================
# FEATURES
# =============================
FEATURES = [
    "mes",
    "hora",
    "precio_unitario",
    "es_fin_semana",
    "hora_pico",
    "producto",
    "categoria_producto",
    "tipo_promocion",
    "tipo_zona",
    "ubicacion_tienda",
    "clima",
    "producto_promocion",
    "temporada"
]


# =============================
# TRAIN MODEL
# =============================
def train_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO TRAIN")

    df = pd.read_sql("SELECT * FROM gold_ml.ventas_dataset", conn)

    if df.empty:
        raise Exception("No hay datos para entrenar")

    df = feature_engineering(df.copy())

    y = df["cantidad_vendida"]
    X = df[FEATURES]

    # =============================
    # PREPROCESSOR (ONE HOT)
    # =============================
    categorical = X.select_dtypes(include="object").columns
    numeric = X.select_dtypes(exclude="object").columns

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical),
            ("num", "passthrough", numeric)
        ]
    )

    # =============================
    # MODELO MEJORADO
    # =============================
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=None,
        random_state=42,
        n_jobs=-1
    )

    pipeline = Pipeline([
        ("preprocess", preprocess),
        ("model", model)
    ])

    # =============================
    # SPLIT
    # =============================
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # =============================
    # TRAIN
    # =============================
    pipeline.fit(X_train, y_train)

    preds = pipeline.predict(X_test)

    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"R2: {r2}")

    log_db(conn, pipeline_name, f"MAE: {mae}")
    log_db(conn, pipeline_name, f"RMSE: {rmse}")
    log_db(conn, pipeline_name, f"R2: {r2}")

    # =============================
    # GUARDAR MODELO SIEMPRE
    # =============================
    os.makedirs("models", exist_ok=True)

    with open("models/model.pkl", "wb") as f:
        pickle.dump(pipeline, f)

    with open("models/features.pkl", "wb") as f:
        pickle.dump(FEATURES, f)

    log_db(conn, pipeline_name, "MODELO GUARDADO")

    return mae, rmse, r2


# =============================
# PREDICT
# =============================
def predict_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO PREDICT")

    with open("models/model.pkl", "rb") as f:
        pipeline = pickle.load(f)

    with open("models/features.pkl", "rb") as f:
        features = pickle.load(f)

    df_pred = pd.read_sql("SELECT * FROM gold_ml.ventas_prediccion", conn)

    df_pred = feature_engineering(df_pred.copy())

    X_new = df_pred[features]

    df_pred["cantidad_predicha"] = pipeline.predict(X_new).round(0).astype(int)

    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE gold_ml.ventas_predicha")
    conn.commit()

    records = df_pred[[
        "fecha","mes","dia_semana","hora","ubicacion_tienda",
        "tipo_zona","producto","categoria_producto",
        "precio_unitario","tipo_promocion","clima","cantidad_predicha"
    ]].values.tolist()

    execute_values(cursor, """
        INSERT INTO gold_ml.ventas_predicha (
            fecha, mes, dia_semana, hora, ubicacion_tienda,
            tipo_zona, producto, categoria_producto,
            precio_unitario, tipo_promocion, clima, cantidad_predicha
        ) VALUES %s
    """, records)

    conn.commit()

    log_db(conn, pipeline_name, "PREDICCIONES GUARDADAS")


# =============================
# MAIN
# =============================
def main():

    pipeline_name = "etl_gold_ml"
    conn = get_connection()

    try:

        mae, rmse, r2 = train_model(conn, pipeline_name)
        predict_model(conn, pipeline_name)

        print("🚀 PIPELINE ML COMPLETO")

    except Exception as e:
        print("ERROR:", e)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
