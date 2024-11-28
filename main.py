import psycopg2

class DataBase:
    def __init__(self) -> None:
        self.database = psycopg2.connect(
            database = '9-Homework',
            user = 'postgres',
            host = 'localhost',
            password = '1'  
        )
        
        
    def manager(self,sql,*args,commit=False,fetchall=False,fetchone=False):
        with self.database as db:
            with db.cursor() as cursor:
                cursor.execute(sql,args)
                if commit:
                    db.commit()
                elif fetchone:
                    return cursor.fetchone()
                elif fetchall:
                    return cursor.fetchall()
                
    def drop_tables(self):
        sqls = ['''
        drop table if exists categories cascade;''',
        '''drop table if exists products cascade;'''
        ]
        for sql in sqls:
            self.manager(sql,commit=True)
    
    def create_tables(self):
        sqls = ['''create table if not exists categories(
            category_id serial primary key,
            categorie_name varchar(50) not null  
            );''',
            
        '''create table if not exists products(
            product_id serial primary key,
            product_name varchar(50) not null,
            price numeric(7,2) not null,
            category_id integer references categories(category_id)
            );''',
            
        '''create table if not exists actors(
            actor_id serial primary key,
            actor_name varchar(50) not null
            )''',
            
        '''create table if not exists movies(
            movie_id serial primary key,
            movie_name varchar(50) not null,
            actor_id integer references actors(actor_id)
            )'''
        ]        
        
        for sql in sqls:
            self.manager(sql,commit=True)   
            
    def insert_into_categories(self,categorie_name):
        sql = '''insert into categories(categorie_name) values (%s)'''
        self.manager(sql,categorie_name,commit=True)
        
    def insert_into_products(self,product_name,price,category_id):
        sql = '''insert into products(product_name,price,category_id) values (%s,%s,%s)'''
        self.manager(sql,product_name,price,category_id,commit=True)
        
    def select_categories(self):
        sql = '''select * from categories;'''
        return self.manager(sql,fetchall=True)
    
    def select_products(self):
        sql = '''select * from products;'''
        return self.manager(sql,fetchall=True)
    
    def select_kategoriya_product_name(self):
        sql = '''select product_name,categorie_name from products join categories on categories.category_id = products.category_id;'''
        return self.manager(sql,fetchall=True)

    def select_cross_join(self):
        sql = '''select * from products cross join categories;'''
        return self.manager(sql,fetchall=True)

    def select_natural_join(self):
        sql = '''select * from products natural join categories; '''
        return self.manager(sql,fetchall=True)

# ===================================
    def insert_into_actors(self,actor_name):
        sql = '''insert into actors(actor_name) values (%s)'''
        self.manager(sql,actor_name,commit=True)
        
    def insert_into_movies(self,movie_name,actor_id):
        sql = '''insert into movies(movie_name,actor_id) values (%s,%s)'''
        self.manager(sql,movie_name,actor_id,commit=True)

    def select_movie_actors(self):
        sql = '''select movie_name,actor_name from movies join actors on movies.actor_id = movies.movie_id;'''
        return self.manager(sql,fetchall=True)
                
db = DataBase()
db.create_tables()
# db.drop_tables()
categories = [
    ('suvlar'),
    ('shirinlik'),
    ('ovqatlar'),
    ('salatlar'),
    ('somsalar'),
]
for category in categories:
    db.insert_into_categories(category)

products = [
    ('cola',12000,1),
    ('shirinli',40000,2),
    ('mastava',30000,3),
    ('olivia',5000,4),
    ('oshqovoqli somsa',30000,5),
    ('zakaz somsa',30000,5),
    ('qizilcha',30000,4),
    ('shorva',25000,3),
    ('rulet',20000,2),
    ('smuzi',10000,1),
    ('baqlajon salat',10000,None),
    ('tandir gril',25000,None),
    ('fish',35000,None),
    ('asarti',15000,None),
    ('gosht somsa',3000,None),
]
for product in products: 
    db.insert_into_products(*product)
    
actors = [
    ('Toxir'),
    ('Toxirov'),
    ('umarhonovich'),
]
for actor in actors:
    db.insert_into_actors(actor)
    
movies = [
    ('jumong',1),
    ('hukmdor usmon',2),
    ('jambobo',1),
    ('chuqur',3),
    ('taqdirlar',3),
    ('tugun',2),
    ('aka',1),
    ('aka-uka grimlar',3),
    ('yana tugashtirildi',1),
]
for movie in movies:
    db.insert_into_movies(*movie)


print(db.select_categories())
print(db.select_products())
print(db.select_kategoriya_product_name())

print(db.select_cross_join())
print(db.select_natural_join())

print(db.select_movie_actors())
