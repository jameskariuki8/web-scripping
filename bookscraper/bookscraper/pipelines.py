from itemadapter import ItemAdapter

class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter =  ItemAdapter(item)

        # Strip all whitespace from the strings
        field_names = adapter.field_names()
        for field_name in field_names:
         if field_name != "description":
           value = adapter.get(field_name)

           if isinstance(value, str):
            adapter[field_name] = value.strip()
           elif isinstance(value, tuple):
            # Assuming the first element of the tuple is the one you want to strip
            adapter[field_name] = value[0].strip()
           else:
            # Handle other cases or raise an error if needed
            adapter[field_name] = str(value).strip()

        

        # Category and product type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
           value = adapter.get(price_key)
           # Remove pound sign and convert to float
           value = value.replace('£', '').replace(',', '')
           adapter[price_key] = float(value)


        # Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        # Reviews --> convert string to numbers
        num_reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_string)

        # Stars --> convert text to numbers
        star_string = adapter.get('stars')
        split_star_array = star_string.split(' ')
        stars_text_value = split_star_array[1].lower()
        if stars_text_value == "zero":
            adapter['stars'] = 0
        elif stars_text_value == "one":
            adapter['stars'] = 1
        elif stars_text_value == "two":
            adapter['stars'] = 2
        elif stars_text_value == "three":
            adapter['stars'] = 3
        elif stars_text_value == "four":
            adapter['stars'] = 4
        elif stars_text_value == "five":
            adapter['stars'] = 5

        return item
    
import mysql.connector

import mysql.connector

class SaveToMySQLPipeline:
    def __init__(self):
        # Establish a connection to MySQL database
        self.conn = mysql.connector.connect(
            host='localhost',
            user='junior',
            password='jr735',
            database='book'
        )
        # Create a cursor to execute commands
        self.cur = self.conn.cursor()
        # Create a table if it doesn't exist
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS book(
                id INT NOT NULL AUTO_INCREMENT,
                url VARCHAR(260),
                title TEXT,
                upc VARCHAR(255),
                product_type VARCHAR(244),
                price_excl_tax DECIMAL,
                price_incl_tax DECIMAL,
                tax DECIMAL,
                price DECIMAL,
                availability INTEGER,
                num_reviews INTEGER,
                stars INTEGER,
                category VARCHAR(255),
                description TEXT,
                PRIMARY KEY (id)
            )
        """)
        # Commit the changes
        self.conn.commit()

    def process_item(self, item, spider):
        # Execute the insert statement
        self.cur.execute("""
            INSERT INTO book (
                url,
                title,
                upc,
                product_type,
                price_excl_tax,
                price_incl_tax,
                tax,
                price,
                availability,
                num_reviews,
                stars,
                category,
                description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            item['url'],
            item['title'],
            item['upc'],
            item['product_type'],
            item['price_excl_tax'],
            item['price_incl_tax'],
            item['tax'],
            item['price'],
            item['availability'],
            item['num_reviews'],
            item['stars'],
            item['category'],
            str(item['description'][0])
        ))
        # Commit the changes
        self.conn.commit()
        return item

    def close_spider(self, spider):
        # Close the cursor and connection to the database
        self.cur.close()
        self.conn.close()
