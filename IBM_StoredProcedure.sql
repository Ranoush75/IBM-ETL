-- Stage comment: create indexes that support the silver batch refresh.
CREATE INDEX IF NOT EXISTS ix_ibm_history_date_last_updated
ON public."IBM_history" ("Date", "Last_updated");

-- Silver comment: keep one clean latest record per business date.
CREATE TABLE IF NOT EXISTS public."IBM" (
    "Date" date PRIMARY KEY,
    "Open" numeric(18, 6),
    "High" numeric(18, 6),
    "Low" numeric(18, 6),
    "Close" numeric(18, 6),
    "Volume" bigint,
    "Last_updated" timestamp,
    etl_loaded_at timestamp DEFAULT now()
);

-- Gold comment: store analytics-ready daily metrics.
CREATE TABLE IF NOT EXISTS public."IBM_gold" (
    "Date" date PRIMARY KEY,
    "Close" numeric(18, 6),
    daily_return numeric(18, 8),
    ma_7 numeric(18, 6),
    ma_30 numeric(18, 6),
    volatility_30 numeric(18, 8),
    volume_ma_30 numeric(18, 2),
    trend_flag text,
    etl_loaded_at timestamp DEFAULT now()
);

-- Silver comment: batch upsert the latest record for each day from the stage table.
CREATE OR REPLACE PROCEDURE refresh_ibm_silver()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH src AS (
        SELECT
            "Date"::date AS "Date",
            "Open"::numeric(18, 6) AS "Open",
            "High"::numeric(18, 6) AS "High",
            "Low"::numeric(18, 6) AS "Low",
            "Close"::numeric(18, 6) AS "Close",
            "Volume"::bigint AS "Volume",
            "Last_updated"::timestamp AS "Last_updated",
            row_number() OVER (
                PARTITION BY "Date"
                ORDER BY "Last_updated"::timestamp DESC
            ) AS rn
        FROM public."IBM_history"
    )
    INSERT INTO public."IBM" (
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Last_updated",
        etl_loaded_at
    )
    SELECT
        "Date",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Last_updated",
        now()
    FROM src
    WHERE rn = 1
    ON CONFLICT ("Date") DO UPDATE
    SET
        "Open" = EXCLUDED."Open",
        "High" = EXCLUDED."High",
        "Low" = EXCLUDED."Low",
        "Close" = EXCLUDED."Close",
        "Volume" = EXCLUDED."Volume",
        "Last_updated" = EXCLUDED."Last_updated",
        etl_loaded_at = now();
END;
$$;

-- Gold comment: rebuild a rolling analytics window and upsert into the gold table.
CREATE OR REPLACE PROCEDURE refresh_ibm_gold()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH base AS (
        SELECT
            "Date",
            "Close",
            "Volume"
        FROM public."IBM"
        WHERE "Date" >= COALESCE(
            (SELECT max("Date") - interval '40 days' FROM public."IBM_gold"),
            date '1900-01-01'
        )
    ),
    returns AS (
        SELECT
            "Date",
            "Close",
            "Volume",
            ("Close" - lag("Close") OVER (ORDER BY "Date"))
                / NULLIF(lag("Close") OVER (ORDER BY "Date"), 0) AS daily_return
        FROM base
    ),
    calc AS (
        SELECT
            "Date",
            "Close",
            daily_return,
            avg("Close") OVER (
                ORDER BY "Date"
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma_7,
            avg("Close") OVER (
                ORDER BY "Date"
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS ma_30,
            stddev_samp(daily_return) OVER (
                ORDER BY "Date"
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS volatility_30,
            avg("Volume") OVER (
                ORDER BY "Date"
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS volume_ma_30
        FROM returns
    )
    INSERT INTO public."IBM_gold" (
        "Date",
        "Close",
        daily_return,
        ma_7,
        ma_30,
        volatility_30,
        volume_ma_30,
        trend_flag,
        etl_loaded_at
    )
    SELECT
        "Date",
        "Close",
        daily_return,
        ma_7,
        ma_30,
        volatility_30,
        volume_ma_30,
        CASE
            WHEN "Close" > ma_30 THEN 'bullish'
            ELSE 'bearish'
        END AS trend_flag,
        now()
    FROM calc
    ON CONFLICT ("Date") DO UPDATE
    SET
        "Close" = EXCLUDED."Close",
        daily_return = EXCLUDED.daily_return,
        ma_7 = EXCLUDED.ma_7,
        ma_30 = EXCLUDED.ma_30,
        volatility_30 = EXCLUDED.volatility_30,
        volume_ma_30 = EXCLUDED.volume_ma_30,
        trend_flag = EXCLUDED.trend_flag,
        etl_loaded_at = now();
END;
$$;

-- Verification comment: review these queries before executing them.
-- SELECT count(*) AS silver_rows, min("Date"), max("Date") FROM public."IBM";
-- SELECT count(*) AS gold_rows, min("Date"), max("Date") FROM public."IBM_gold";
-- SELECT * FROM public."IBM_gold" ORDER BY "Date" DESC LIMIT 10;

