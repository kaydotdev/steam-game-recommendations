CREATE TABLE IF NOT EXISTS "Games"
(
    app_id         INTEGER NOT NULL
        CONSTRAINT "Games_pk"
            PRIMARY KEY,
    title          TEXT    NOT NULL,
    description    TEXT,
    steam_deck     BOOLEAN NOT NULL,
    date_release   DATE    NOT NULL,
    win            BOOLEAN NOT NULL,
    mac            BOOLEAN NOT NULL,
    linux          BOOLEAN NOT NULL,
    rating         TEXT    NOT NULL,
    positive_ratio INTEGER NOT NULL,
    user_reviews   INTEGER NOT NULL,
    price_final    NUMERIC NOT NULL,
    price_original NUMERIC NOT NULL,
    discount       NUMERIC NOT NULL,
    tags           TEXT
);

CREATE INDEX IF NOT EXISTS "Games_date_release_index"
    ON "Games" (date_release);

CREATE TABLE IF NOT EXISTS "Users"
(
    userprofile BIGINT  NOT NULL
        CONSTRAINT "Users_pk"
            PRIMARY KEY,
    products    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS "Recommendations"
(
    app_id         INTEGER NOT NULL
        CONSTRAINT "Recommendations_Games_app_id_fk"
            REFERENCES "Games",
    userprofile    BIGINT  NOT NULL
        CONSTRAINT "Recommendations_Users_userprofile_fk"
            REFERENCES "Users",
    date           DATE    NOT NULL,
    is_recommended BOOLEAN NOT NULL,
    helpful        INTEGER NOT NULL,
    funny          INTEGER NOT NULL,
    hours          NUMERIC NOT NULL,
    CONSTRAINT "Recommendations_pk"
        PRIMARY KEY (app_id, userprofile, date)
);

