-- Test data preparation: verify Spark SQL source tables and create the analysis view.

USE bigdata_ana_test;

SELECT 'movies' AS table_name, COUNT(*) AS row_count FROM movies
UNION ALL
SELECT 'ratings' AS table_name, COUNT(*) AS row_count FROM ratings
UNION ALL
SELECT 'tags' AS table_name, COUNT(*) AS row_count FROM tags
UNION ALL
SELECT 'links' AS table_name, COUNT(*) AS row_count FROM links;

CREATE OR REPLACE VIEW v_movies_ratings AS
SELECT
    m.movieId,
    m.title,
    m.genres,
    r.userId,
    r.rating,
    FROM_UNIXTIME(r.`timestamp`) AS rating_time
FROM movies m
JOIN ratings r ON m.movieId = r.movieId;
