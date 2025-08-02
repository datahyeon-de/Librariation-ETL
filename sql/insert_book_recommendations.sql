INSERT INTO book_recommendations (
    isbn13, rec_isbn13, addition_symbol, vol
) VALUES (
    %(isbn13)s, %(rec_isbn13)s, %(addition_symbol)s, %(vol)s
) ON DUPLICATE KEY UPDATE
    addition_symbol = VALUES(addition_symbol),
    vol = VALUES(vol),
    updated_at = CURRENT_TIMESTAMP