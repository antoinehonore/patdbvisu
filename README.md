

# New category
See `queries/set_view.sql` and `queries/drop_view.sql`


## Associated to ids__uid only
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


## Associated with sequential data 
### monitor, clinisoft
- Add a view listing the interval and patients of interest (columns: ids__uid, ids__interval) 
- Add a case in the view with binary column ids__uid categories 
- Add a view listing the unique patients of interest (column: ids__uid)
- Add a case in the view with binary column ids__uid categories


### takecare
See `bin/views.py`
- Add a key/value pair in dictionary `d`. The key should the name you want for your category. 
The value should be a regexp to apply to the column names of the takecare table.
- 


