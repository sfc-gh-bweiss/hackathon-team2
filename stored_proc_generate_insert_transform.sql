/*
Stored procedure to dynamically concatenate
INSERT statement that will allow 
populating a target table with data from another table,
but will reorder the columns.

In other words:
1. Via Streamlit, end users upload an Excel file with columns that are not in the correct order.
2. Streamlit app calls this Stored Proc.
3. Below Stored Proc makes an INSERT statement to populate a target table with correct order of columns.

*/

create or replace procedure generate_insert_transform(source_table varchar, target_table varchar)
  returns varchar
  language sql
  EXECUTE AS CALLER
  as
    declare
      source_cols varchar;
      target_cols varchar;

    begin
        -- Initialize variables.
        let my_var := :source_table;
        source_cols := '';
        target_cols := '';
        
        -- Generate SQL statement to make a temp table.
        -- This is a work around for issues with running the TABLE function directly.
        let the_sql := 'create or replace table tags_temp as select * from table(information_schema.tag_references_all_columns(''' || my_var || ''', ''table''))  t where TAG_NAME=''TARGET_COLUMN''';
        execute immediate the_sql;
    
        -- Generate a cursor from the meta data for purposes of making an INSERT statement.
        let  res resultset := ( select * from TAGS_TEMP);
        let cur cursor for res;

        -- Make the first part of the INSERT statement.
        let tbl_transform varchar := 'insert into '|| target_table || '(';
        
        -- Initialize a counter, then loop through the results of the cursor
        -- in order to generate all the columns in the INSERT.
        let y number := 0;        
       for row_variable in cur do
            if (y = 0) then
                source_cols :=  '"' || row_variable.COLUMN_NAME;
                target_cols := row_variable.TAG_VALUE;
            else
                source_cols := source_cols || ',  "' || row_variable.COLUMN_NAME || '"';
                target_cols := target_cols || ',' || row_variable.TAG_VALUE ;
            
            end if;
            y := y + 1;
        end for;
        
        -- Now finish up the end of the INSERT statement
        tbl_transform := tbl_transform || target_cols || ') as select '|| source_cols || ' from ' || :source_table;
   
        return tbl_transform;
    end;
    
/*

SQL statements to test stored proc.

*/


drop table tags_temp;
call generate_insert_transform('TESTBOOK1','TESTBOOKTARGET');

desc table testbooktarget;

select * from table(information_schema.tag_references_all_columns('TESTBOOK1', 'table')) t where TAG_NAME='TARGET_COLUMN';
select * from tags_temp;

create tag target_column;

-- Make tags for use with testing stored proc.
alter table TESTBOOK1 modify column "Col A" set tag target_column='"COL C"';
alter table TESTBOOK1 modify column "Col B" set tag target_column='"COL D"';
select system$get_tag('target_column','TESTBOOK1','column') ;
call generate_insert_transform('TESTBOOK1','TESTBOOKTARGET');
