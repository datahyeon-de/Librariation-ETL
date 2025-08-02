INSERT IGNORE INTO book (
    book_id,
    isbn13,
    addition_symbol,
    vol,
    bookname,
    authors,
    publisher,
    publication_year,
    class_no,
    class_nm,
    bookImageURL,
    bookDtlUrl
)
SELECT DISTINCT
    CONCAT(
        raw.isbn13, 
        '_', 
        COALESCE(NULLIF(raw.addition_symbol, ''), 'NOADD'),
        '_',
        COALESCE(NULLIF(raw.vol, ''), 'NOVOL')
    ) AS book_id,
    raw.isbn13,
    raw.addition_symbol,
    raw.vol,
    raw.bookname,
    raw.authors,
    raw.publisher,
    raw.publication_year,
    raw.class_no,
    raw.class_nm,
    raw.bookImageURL,
    raw.bookDtlUrl
FROM raw_loan_item_srch raw
WHERE raw.isbn13 IS NOT NULL
  AND raw.created_at > (
      SELECT COALESCE(MAX(b.updated_at), '1900-01-01 00:00:00') 
      FROM book b
  );