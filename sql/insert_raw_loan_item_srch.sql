INSERT INTO raw_loan_item_srch (
    start_dt, end_dt, age, no, ranking, bookname, authors, publisher, publication_year, isbn13, addition_symbol, vol, class_no, class_nm, bookImageURL, bookDtlUrl, loan_count
) VALUES (
    %(start_dt)s, %(end_dt)s, %(age)s, %(no)s, %(ranking)s, %(bookname)s, %(authors)s, %(publisher)s, %(publication_year)s, %(isbn13)s, %(addition_symbol)s, %(vol)s, %(class_no)s, %(class_nm)s, %(bookImageURL)s, %(bookDtlUrl)s, %(loan_count)s
) ON DUPLICATE KEY UPDATE
    ranking = VALUES(ranking),
    bookname = VALUES(bookname),
    authors = VALUES(authors),
    publisher = VALUES(publisher),
    publication_year = VALUES(publication_year),
    class_no = VALUES(class_no),
    class_nm = VALUES(class_nm),
    bookImageURL = VALUES(bookImageURL),
    bookDtlUrl = VALUES(bookDtlUrl),
    loan_count = VALUES(loan_count)