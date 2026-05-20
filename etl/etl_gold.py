def predict_model(conn, pipeline_name):

    log_db(conn, pipeline_name, "INICIO PREDICT")

    # =============================
    # CARGAR MODELO
    # =============================
    with open("models/model.pkl", "rb") as f:
        pipeline = pickle.load(f)

    with open("models/features.pkl", "rb") as f:
        features = pickle.load(f)

    # =============================
    # DATA
    # =============================
    df_pred = pd.read_sql(
        "SELECT * FROM gold_ml.ventas_prediccion",
        conn
    )

    df_pred = feature_engineering(df_pred.copy())

    X_new = df_pred[features]

    # =============================
    # PREDICCIÓN
    # =============================
    df_pred["cantidad_predicha"] = (
        pipeline.predict(X_new)
        .round(0)
        .astype(int)
    )

    # 🔥 FIX CRÍTICO: evitar error tipo TIME vs INT
    df_pred["hora"] = df_pred["hora"].astype(int)

    # =============================
    # INSERT EN BD
    # =============================
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

    log_db(conn, pipeline_name, "PREDICCIONES GUARDADAS")
