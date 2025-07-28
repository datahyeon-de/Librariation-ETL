INSERT IGNORE INTO loan_item_srch (
    id, startDt, endDt, age, ranking, bookname, authors, publisher, publication_year, isbn13, addition_symbol, vol, class_no, class_nm, bookImageURL, bookDtlUrl, loan_count
) VALUES (
    %(id)s, %(startDt)s, %(endDt)s, %(age)s, %(ranking)s, %(bookname)s, %(authors)s, %(publisher)s, %(publication_year)s, %(isbn13)s, %(addition_symbol)s, %(vol)s, %(class_no)s, %(class_nm)s, %(bookImageURL)s, %(bookDtlUrl)s, %(loan_count)s
) 