import sys
import os

sys.path.append(os.path.abspath("./.secrets"))

import pandas as pd
import numpy as np
import datetime
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)

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

    df["hora_pico"] = df["hora"].apply(
        lambda x: 1 if (12 <= x <= 14 or 18 <= x <= 21) else 0
    )

    df["producto_promocion"] = (
        df["producto"].astype(str) + "_" + df["tipo_promocion"].astype(str)
    )

    df["temporada"] = pd.cut(
        df["mes"],
        bins=[0, 3, 6, 9, 12],
        labels=["Q1", "Q2", "Q3", "Q4"]
    ).astype(str)

    return df


# =============================
# FEATURES DEL MODELO
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
# TRAIN
# =============================
def train_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO TRAIN")

    df = pd.read_sql("SELECT * FROM gold_ml.ventas_dataset", conn)

    df_model = feature_engineering(df.copy())

    y = df_model["cantidad_vendida"]
    X = df_model[FEATURES]

    # encoding
    encoders = {}
    for col in X.select_dtypes(include="object").columns:
        le = LabelEncoder()
        X[col] = le.fit_transform(X[col].astype(str))
        encoders[col] = le

    # split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # modelo
    model = RandomForestRegressor(
        n_estimators=300,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train, y_train)

    preds = model.predict(X_test)

    # métricas
    mae = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2 = r2_score(y_test, preds)

    print(f"MAE: {mae}")
    print(f"RMSE: {rmse}")
    print(f"R2: {r2}")

    log_db(conn, pipeline_name, f"MAE: {mae}")
    log_db(conn, pipeline_name, f"RMSE: {rmse}")
    log_db(conn, pipeline_name, f"R2: {r2}")

    if r2 < 0.5:
        raise Exception("Modelo muy débil")

    # guardar
    os.makedirs("models", exist_ok=True)

    with open("models/model.pkl", "wb") as f:
        pickle.dump(model, f)

    with open("models/encoders.pkl", "wb") as f:
        pickle.dump(encoders, f)

    log_db(conn, pipeline_name, "MODELO GUARDADO")

    return mae, rmse, r2


# =============================
# PREDICT
# =============================
def predict_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO PREDICT")

    with open("models/model.pkl", "rb") as f:
        model = pickle.load(f)

    with open("models/encoders.pkl", "rb") as f:
        encoders = pickle.load(f)

    df_pred = pd.read_sql("SELECT * FROM gold_ml.ventas_prediccion", conn)

    df_model = feature_engineering(df_pred.copy())
    X_new = df_model[FEATURES]

    # encoding consistente
    for col, le in encoders.items():
        X_new[col] = X_new[col].astype(str)
        X_new[col] = X_new[col].apply(
            lambda x: le.transform([x])[0] if x in le.classes_ else -1
        )

    df_pred["cantidad_predicha"] = model.predict(X_new).round(0).astype(int)

    cursor = conn.cursor()

    cursor.execute("TRUNCATE TABLE gold_ml.ventas_predicha")
    conn.commit()

    records = df_pred[[
        "fecha",
        "mes",
        "dia_semana",
        "hora",
        "ubicacion_tienda",
        "tipo_zona",
        "producto",
        "categoria_producto",
        "precio_unitario",
        "tipo_promocion",
        "clima",
        "cantidad_predicha"
    ]].values.tolist()

    execute_values(cursor, """
        INSERT INTO gold_ml.ventas_predicha (
            fecha, mes, dia_semana, hora,
            ubicacion_tienda, tipo_zona,
            producto, categoria_producto,
            precio_unitario, tipo_promocion,
            clima, cantidad_predicha
        ) VALUES %s
    """, records)

    conn.commit()

    log_db(conn, pipeline_name, "PREDICCIONES GUARDADAS")


# =============================
# MAIN
# =============================
def main():

    pipeline_name = "etl_gold_ml"

    escribir_log("INICIO PIPELINE GOLD ML")

    conn = get_connection()
    cursor = conn.cursor()

    run_id = None

    try:

        cursor.execute("""
            INSERT INTO meta.pipeline_run
            (pipeline_name, fecha_inicio, estado)
            VALUES (%s, now(), %s)
            RETURNING id
        """, (pipeline_name, "RUNNING"))

        run_id = cursor.fetchone()[0]
        conn.commit()

        log_db(conn, pipeline_name, "PIPELINE START")

        mae, rmse, r2 = train_model(conn, pipeline_name)
        predict_model(conn, pipeline_name)

        cursor.execute("""
            UPDATE meta.pipeline_run
            SET fecha_fin = now(),
                estado = %s,
                filas_procesadas = (
                    SELECT COUNT(*) FROM gold_ml.ventas_predicha
                )
            WHERE id = %s
        """, ("OK", run_id))

        conn.commit()

        log_db(conn, pipeline_name, f"PIPELINE OK | MAE={mae} RMSE={rmse} R2={r2}")

        print("🚀 PIPELINE ML COMPLETO")

    except Exception as e:

        conn.rollback()

        print("ERROR:", e)
        log_db(conn, pipeline_name, f"ERROR {e}")

        if run_id:
            cursor.execute("""
                UPDATE meta.pipeline_run
                SET fecha_fin = now(),
                    estado = 'ERROR'
                WHERE id = %s
            """, (run_id,))
            conn.commit()

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
