-- Query 1
SELECT 
    title,
    REGEXP_REPLACE(author, '^By\s*', '', 'i') AS cleaned_author
FROM wired_articles;

-- Query 2
SELECT 
    REGEXP_REPLACE(author, '^By\s*', '', 'i') AS cleaned_author,
    COUNT(*) AS total_articles
FROM wired_articles
GROUP BY cleaned_author
ORDER BY total_articles DESC
LIMIT 3;

-- Query 3
SELECT 
    title,
    description,
    author,
    url
FROM wired_articles
WHERE 
    title ILIKE '%AI%'
    OR title ILIKE '%Climate%'
    OR title ILIKE '%Security%'
    OR description ILIKE '%AI%'
    OR description ILIKE '%Climate%'
    OR description ILIKE '%Security%';