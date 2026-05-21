import sys
import os

sys.path.append(os.path.abspath("./.secrets"))

import pandas as pd
import numpy as np
import datetime
import pickle

from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    r2_score
)
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
            VALUES (%s, %s)
        """, (pipeline, msg))
        conn.commit()
    except Exception as e:
        print("Error log_db:", e)


# =============================
# LIMPIEZA TEXTO
# =============================

# BUG CORREGIDO #1:
# El original hacía .str.lower() a todas las columnas categóricas,
# incluyendo "clima". Esto causaba que el OneHotEncoder entrenara
# con valores en minúscula ("soleado", "nublado") pero la interfaz
# Streamlit enviaba los valores con mayúscula ("Soleado", "Nublado").
# Al recibir un valor desconocido, OneHotEncoder(handle_unknown="ignore")
# lo silenciaba poniendo todos los dummies en 0, como si clima no existiera.
# SOLUCIÓN: normalizar a minúsculas aquí Y en la interfaz (build_features).

def clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "producto",
        "categoria_producto",
        "tipo_promocion",
        "tipo_zona",
        "ubicacion_tienda",
        "clima",
        "dia_semana",
    ]
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .str.lower()   # normalización consistente
            )
    return df


# =============================
# FEATURE ENGINEERING
# =============================

def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    df["mes"] = (
        pd.to_numeric(df["mes"], errors="coerce")
        .fillna(1)
        .astype(int)
    )

    # BUG CORREGIDO #2:
    # El original hacía pd.to_datetime(df["hora"].astype(str)).dt.hour
    # Cuando "hora" ya es un entero (ej: 14), to_datetime lo interpreta
    # como milisegundos desde epoch → año 1970, hora 0.
    # Ejemplo: pd.to_datetime("14") → 1970-01-01 00:00:00.000000014
    # Resultado: df["hora"] siempre era 0, inutilizando hora y hora_pico.
    # SOLUCIÓN: convertir directamente a int, con fallback seguro.
    df["hora"] = (
        pd.to_numeric(df["hora"], errors="coerce")
        .fillna(0)
        .astype(int)
        .clip(0, 23)   # garantizar rango válido
    )

    df["precio_unitario"] = (
        pd.to_numeric(df["precio_unitario"], errors="coerce")
        .fillna(0)
        .round(2)
    )

    df["dia_mes"]   = df["fecha"].dt.day
    df["trimestre"] = df["fecha"].dt.quarter

    # BUG CORREGIDO #3:
    # El original comparaba dia_semana con ["saturday", "sunday"] en inglés,
    # pero clean_text_columns ya convierte a minúsculas. Si el dataset tiene
    # los días en español ("sábado", "domingo") nunca coincidía → es_fin_semana = 0 siempre.
    # SOLUCIÓN: derivar es_fin_semana directamente desde la fecha (método infalible),
    # sin depender del texto de dia_semana.
    df["es_fin_semana"] = df["fecha"].dt.weekday.isin([5, 6]).astype(int)

    df["hora_pico"] = df["hora"].apply(
        lambda x: 1 if (12 <= x <= 14 or 18 <= x <= 21) else 0
    )

    df["temporada"] = pd.cut(
        df["mes"],
        bins=[0, 3, 6, 9, 12],
        labels=["Q1", "Q2", "Q3", "Q4"],
    ).astype(str)

    df["producto_promocion"] = (
        df["producto"].astype(str)
        + "_"
        + df["tipo_promocion"].astype(str)
    )

    return df


# =============================
# FEATURES
# =============================

FEATURES = [
    "mes",
    "dia_mes",
    "trimestre",
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
    "temporada",
    "producto_promocion",
]


# =============================
# TRAIN MODEL
# =============================

def train_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO TRAIN")

    df = pd.read_sql("SELECT * FROM gold_ml.ventas_dataset", conn)

    if df.empty:
        raise Exception("No hay datos para entrenar")

    df = clean_text_columns(df)
    df = feature_engineering(df.copy())

    y = df["cantidad_vendida"]
    X = df[FEATURES]

    categorical = X.select_dtypes(
        include=["object", "string", "category"]
    ).columns.tolist()

    numeric = X.select_dtypes(
        exclude=["object", "string", "category"]
    ).columns.tolist()

    preprocess = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical),
            ("num", "passthrough", numeric),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=150,
        max_depth=20,
        min_samples_split=5,
        min_samples_leaf=2,
        max_features="sqrt",
        bootstrap=True,
        random_state=42,
        n_jobs=-1,
    )

    pipeline = Pipeline([
        ("preprocess", preprocess),
        ("model", model),
    ])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        shuffle=True,
    )

    pipeline.fit(X_train, y_train)
    preds = pipeline.predict(X_test)

    mae  = mean_absolute_error(y_test, preds)
    rmse = np.sqrt(mean_squared_error(y_test, preds))
    r2   = r2_score(y_test, preds)

    print("\n========================")
    print("MÉTRICAS MODELO")
    print("========================")
    print(f"MAE : {mae:.4f}")
    print(f"RMSE: {rmse:.4f}")
    print(f"R2  : {r2:.4f}")

    log_db(conn, pipeline_name, f"MAE: {mae:.4f}")
    log_db(conn, pipeline_name, f"RMSE: {rmse:.4f}")
    log_db(conn, pipeline_name, f"R2: {r2:.4f}")

    # Importancia de variables
    try:
        feature_names = pipeline.named_steps["preprocess"].get_feature_names_out()
        importances   = pipeline.named_steps["model"].feature_importances_

        importance_df = pd.DataFrame({
            "feature":    feature_names,
            "importance": importances,
        }).sort_values("importance", ascending=False)

        print("\nTOP 15 VARIABLES")
        print(importance_df.head(15).to_string(index=False))

        # Verificación explícita de clima
        clima_features = importance_df[importance_df["feature"].str.contains("clima")]
        if clima_features.empty:
            print("\n⚠️  ADVERTENCIA: 'clima' no aparece entre las features del modelo.")
        else:
            print(f"\n✅ Importancia total de 'clima': "
                  f"{clima_features['importance'].sum():.4f}")
            print(clima_features.to_string(index=False))

    except Exception as e:
        print("Error feature importance:", e)

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

    if df_pred.empty:
        raise Exception("No hay datos para predicción")

    df_pred = clean_text_columns(df_pred)
    df_pred = feature_engineering(df_pred.copy())

    X_new = df_pred[features]

    df_pred["cantidad_predicha"] = (
        pipeline.predict(X_new)
        .round(0)
        .astype(int)
        .clip(1)   # mínimo 1 unidad
    )

    # BUG CORREGIDO #4:
    # El original convertía hora a time object (HH:MM:SS) para insertar en DB,
    # pero durante el entrenamiento hora es un int (0-23).
    # Esto causaba inconsistencia si se reutilizaba df_pred después.
    # SOLUCIÓN: mantener hora como int durante todo el proceso;
    # solo formatear al momento exacto de inserción.
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE gold_ml.ventas_predicha")
    conn.commit()

    records = [
        (
            row["fecha"],
            int(row["mes"]),
            row["dia_semana"],
            # Formatear hora solo aquí, en el momento de insertar
            datetime.time(int(row["hora"]), 0, 0),
            row["ubicacion_tienda"],
            row["tipo_zona"],
            row["producto"],
            row["categoria_producto"],
            float(row["precio_unitario"]),
            row["tipo_promocion"],
            row["clima"],
            int(row["cantidad_predicha"]),
        )
        for _, row in df_pred.iterrows()
    ]

    execute_values(cursor, """
        INSERT INTO gold_ml.ventas_predicha (
            fecha,
            mes,
            dia_semana,
            hora,
            ubicacion_tienda,
            tipo_zona,
            producto,
            categoria_producto,
            precio_unitario,
            tipo_promocion,
            clima,
            cantidad_predicha
        ) VALUES %s
    """, records)

    conn.commit()
    log_db(conn, pipeline_name, f"PREDICCIONES GUARDADAS: {len(records)} registros")


# =============================
# MAIN
# =============================

def main():
    pipeline_name = "etl_gold_ml"

    escribir_log("INICIO PIPELINE GOLD ML")

    conn = get_connection()

    try:
        train_model(conn, pipeline_name)
        predict_model(conn, pipeline_name)
        print("\n🚀 PIPELINE ML COMPLETO")

    except Exception as e:
        print("\nERROR:", e)
        log_db(conn, pipeline_name, f"ERROR: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
