SELECT 
    isbn13, book_id
FROM 
    book
WHERE 
    description IS NULL
    AND isbn13 IS NOT NULL
LIMIT %s