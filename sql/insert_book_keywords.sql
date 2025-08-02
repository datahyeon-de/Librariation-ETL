INSERT INTO book_keywords (
    isbn13, word, weight
) VALUES (
    %(isbn13)s, %(word)s, %(weight)s
) ON DUPLICATE KEY UPDATE
    weight = VALUES(weight),
    updated_at = CURRENT_TIMESTAMP