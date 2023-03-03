"""
create a sqlite database from the yelp dataset

"""
from utils import load_config, flatten_dict, file_line_generator
import pandas as pd
from pathlib import Path
import re

from sqlalchemy import create_engine, MetaData, Table, Column, ForeignKey, UniqueConstraint, select, update
from sqlalchemy.dialects.sqlite import Insert, INTEGER, VARCHAR, TEXT, REAL
from sqlalchemy.exc import IntegrityError



class YelpDataBase:
    """Builds a sqlite database from the yelp dataset
    """
    def __init__(self, config_file_path:str='config.yaml') -> None:
        """Loads the file paths from the config file and connects to the database

        Args:
            config_file_path (str, optional): path to the config.yaml file. Defaults to 'config.yaml'.
        """

        config_obj = load_config(config_file_path, 'database')
        self.database_path = config_obj['database_file_path']
        self.raw_data_folder_path = config_obj['raw_data_folder_path']
        self.connect()

    def connect(self) -> None:
        """creats a sqlalchemy engine and metadata object i.e connects to the database file
        """
        self.engine = create_engine(f'sqlite:///{self.database_path}')
        self.meta_data = MetaData(bind=self.engine)

    def _create_tables_if_not_exist(self) -> None:
        """creates all tables if they don't already exist in the database

            Tables:
                business - contains businesses and business attributes\n
                days - mon - fri\n
                hours - business hours (open - close) 24 hour clock\n
                users - contains users and user attributes\n
                elite - years elite for each user\n
                reviews - reviews and review attributes\n
                tips - tips and tip attributes\n
                checkins - checkin datetimes\n
                category - business categories\n
                category_business - passthrough table connecting categories and businesses\n
                friends - user friend connections\n
                attributes - business attributes\n
                business_attributes - passthrough table connecting businesses and attributes\n
                photos - photos and photo attributes
        """
        # business
        Table(
            'business',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('business_id_str', VARCHAR(32), unique=True),
            Column('name', VARCHAR(256)),
            Column('address', VARCHAR(128)),
            Column('city', VARCHAR(128)),
            Column('state', VARCHAR(64)),
            Column('postal_code', VARCHAR(8)),
            Column('latitude', REAL()),
            Column('longitude', REAL()),
            Column('stars', REAL()),
            Column('review_count', INTEGER()),
            Column('is_open', INTEGER())
        )

        # days
        Table(
            'days',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True),
            Column('day', VARCHAR(32), unique=True)
        )

        # hours
        Table(
            'hours',
            self.meta_data,
            Column('business_id', ForeignKey('business.id')),
            Column('day_id', ForeignKey('days.id')),
            Column('open_hours', VARCHAR(128)),
            UniqueConstraint('business_id', 'day_id')
        )

        # users
        Table(
            'users',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('user_id_str', VARCHAR(32), unique=True),
            Column('name', VARCHAR(128)),
            Column('review_count', INTEGER()),
            Column('yelping_since', VARCHAR(32)),
            Column('useful', INTEGER()),
            Column('funny', INTEGER()),
            Column('cool', INTEGER()),
            Column('fans', INTEGER()),
            Column('friend_count', INTEGER()),
            Column('average_stars', REAL()),
            Column('compliment_hot', INTEGER()),
            Column('compliment_more', INTEGER()),
            Column('compliment_profile', INTEGER()),
            Column('compliment_cute', INTEGER()),
            Column('compliment_list', INTEGER()),
            Column('compliment_note', INTEGER()),
            Column('compliment_plain', INTEGER()),
            Column('compliment_cool', INTEGER()),
            Column('compliment_funny', INTEGER()),
            Column('compliment_writer', INTEGER()),
            Column('compliment_photos', INTEGER())

        )
        # elite
        Table(
            'elite',
            self.meta_data,
            Column('user_id', ForeignKey('users.id')),
            Column('year', INTEGER())
        )

        # reviews
        Table(
            'reviews',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('review_id_str', VARCHAR(32), unique=True),
            Column('user_id', ForeignKey('users.id')),
            Column('business_id', ForeignKey('business.id')),
            Column('stars', INTEGER()),
            Column('date', VARCHAR(32)),
            Column('text', TEXT()),
            Column('useful', INTEGER()),
            Column('funny', INTEGER()),
            Column('cool', INTEGER())
        )

        # tips
        Table(
            'tips',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('business_id', ForeignKey('business.id')),
            Column('user_id', ForeignKey('users.id')),
            Column('text', TEXT()),
            Column('date', VARCHAR(32)),
            Column('compliment_count', INTEGER())
        )

        # checkins
        Table(
            'checkins',
            self.meta_data,
            Column('business_id', ForeignKey('business.id')),
            Column('date', TEXT())
        )

        # category
        Table(
            'category',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('name', VARCHAR(128), unique=True)
        )

        # category - business - passthrough
        Table(
            'category_business',
            self.meta_data,
            Column('category_id', ForeignKey('category.id')),
            Column('business_id', ForeignKey('business.id')),
            UniqueConstraint('category_id', 'business_id')
        )

        # friends - user user - passthrough
        Table(
            'friends',
            self.meta_data,
            Column('user1_id', ForeignKey('users.id')),
            Column('user2_id', ForeignKey('users.id')),
            UniqueConstraint('user1_id', 'user2_id')
        )

        # attributes
        Table(
            'attributes',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('name', VARCHAR(128), unique=True)
        )

        # business - attributes - passthrough
        Table(
            'business_attributes',
            self.meta_data,
            Column('attribute_id', ForeignKey('attributes.id')),
            Column('business_id', ForeignKey('business.id')),
            Column('value', VARCHAR(128)),
            UniqueConstraint('attribute_id', 'business_id')
        )

        # photos
        Table(
            'photos',
            self.meta_data,
            Column('id', INTEGER(), primary_key=True, autoincrement=True),
            Column('photo_id_str', VARCHAR(32), unique=True),
            Column('business_id', ForeignKey('business.id')),
            Column('caption', TEXT()),
            Column('label', TEXT())
        )


        self.meta_data.create_all(checkfirst=True)

    def verbose_loading(self, file_name:str, connecting_frineds:bool, verbose:bool):
        """function to show progress when populating the database

        Args:
            file_name (str): the file name of the file currently being loaded
            connecting_frineds (bool): if this function is being called to populate the friends table
            verbose (bool): if the verbose argumetn was passed into the parent function
        """
        if verbose:
            if connecting_frineds:
                print('Connecting Users')
            else:
                print(f'Loading: {file_name}')




    def create_full_database(self, verbose:bool=False, include_photos:bool=True) -> None:
        """reads the files from the raw data folder checks if the 
            required ones are present and populates the database
            busiess, user, and review json files are required.

        Args:
            verbose (bool, optional): whether to print out progress. Defaults to False.
            include_photos (bool, optional): wether to include the photos table. Defaults to True.
        """
        self._create_tables_if_not_exist()


        base_path = Path(self.raw_data_folder_path)

        business_ok = False
        user_ok = False
        review_ok = False
        checkin_ok = False
        tip_ok =False
        photo_ok = False

        for path in base_path.iterdir():
            if path.suffix == '.json':
                if 'business' in path.name:
                    business_file_path = path
                    business_ok = True
                elif 'user' in path.name:
                    user_file_path = path
                    user_ok = True
                elif 'review' in path.name:
                    review_file_path = path
                    review_ok = True
                elif 'checkin' in path.name:
                    checkin_file_path = path
                    checkin_ok = True
                elif 'tip' in path.name:
                    tip_file_path = path
                    tip_ok = True
                elif 'photo' in path.name:
                    photo_file_path = path
                    photo_ok = True
                else:
                    pass

       
        if business_ok and user_ok and review_ok:

            self._load_business_json(business_file_path, verbose)

            self._load_users_json(user_file_path, verbose)
            
            self._connect_users(user_file_path, verbose)

            self._load_review_json(review_file_path, verbose)
        else:
            print('failed! data folder must contain business, user, and review json files')
            return -1
        
        if checkin_ok:
            self._load_checkin_json(checkin_file_path, verbose)

        if tip_ok:
            self._load_tip_json(tip_file_path, verbose)

        if photo_ok and include_photos:
            self._load_photos_json(photo_file_path, verbose)

    def _initialize_days_table(self) -> dict:
        """initializ the days table, monday through sunday

        Returns:
            dict: {day[str]: id[int]} dict for use in populating business table
        """
        days_table = Table('days', self.meta_data, autoload_with=self.engine)
        day_id_days_dict = {day: _id for _id, day in enumerate(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], start=1)}

        with self.engine.begin() as conn:
            days_insert_statement = Insert(days_table).values(
                [{'id': _id, 'day': day} for _id, day in enumerate(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], start=1)]
            ).on_conflict_do_nothing()
            conn.execute(days_insert_statement)
            
        return day_id_days_dict
    
    def _get_add_user_id(self, connection, table, id_str) -> int:
        """check if a user is in the users table, if yes return the users id,
            else add the user to the table and return the id.

        Args:
            connection: sqlalchemy connection
            table: sqlalchemy users table
            id_str (str): user string id

        Returns:
            int: integer user id
        """
        id_statement = select([table.c.id]).where(table.c.user_id_str == id_str)
        user_id = connection.execute(id_statement).fetchall()
        try:
            user_id = user_id[0][0]
        except IndexError:
            user_id_insert = Insert(table).values(user_id_str=id_str)
            user_id = connection.execute(user_id_insert).inserted_primary_key[0]
        return user_id
    
    def _get_add_business_id(self, connection, table, id_str) -> int:
        """check if a business is in the business table, if yes return the business id,
            else add the business to the table and return the id.

        Args:
            connection: sqlalchemy connection
            table: sqlalchemy business table
            id_str (str): business string id

        Returns:
            int: integer business id
        """
        id_statement = select([table.c.id]).where(table.c.business_id_str == id_str)
        business_id = connection.execute(id_statement).fetchall()
        try:
            business_id = business_id[0][0]
        except IndexError:
            business_id_insert = Insert(table).values(user_id_str=id_str)
            business_id = connection.execute(business_id_insert).inserted_primary_key[0]
        return business_id

    def _load_business_json(self, business_file_path:Path, verbose:bool):
        """populates the business and business associated tables from business.json

        Args:
            business_file_path (Path): file Path object for business.json
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(business_file_path.name, False, verbose)

        business_table = Table('business', self.meta_data, autoload_with=self.engine)
        category_table = Table('category', self.meta_data, autoload_with=self.engine)
        category_business_table = Table('category_business', self.meta_data, autoload_with=self.engine)
        attributes_table = Table('attributes', self.meta_data, autoload_with=self.engine)
        business_attributes_table = Table('business_attributes', self.meta_data, autoload_with=self.engine)
        hours_table = Table('hours', self.meta_data, autoload_with=self.engine)

        day_id_dict = self._initialize_days_table()

        with self.engine.begin() as conn:
            for business in file_line_generator(business_file_path):
                    
                    # business table
                    business_statement = Insert(business_table).values(
                        business_id_str=business['business_id'].strip(),
                        name=business['name'].strip(),
                        address=business['address'].strip(),
                        city=business['city'].strip(),
                        state=business['state'].strip(),
                        postal_code=business['postal_code'].strip(),
                        latitude=business['latitude'],
                        longitude=business['longitude'],
                        stars=business['stars'],
                        review_count=business['review_count'],
                        is_open=business['is_open']
                    )
                    try:
                        business_id = conn.execute(business_statement).inserted_primary_key[0]
                    except IntegrityError:
                        continue
                    # category table
                    if business['categories'] is not None:
                        business_categories = [b.strip() for b in business['categories'].split(', ')]
                        category_statement = Insert(category_table).values(
                            [{'name': category.strip()} for category in business_categories]
                        ).on_conflict_do_nothing()

                        conn.execute(category_statement)

                        category_ids = conn.execute(select([category_table.c.id]).where(category_table.c.name.in_(business_categories))).fetchall()

                        # category business table
                        category_business_statement = Insert(category_business_table).values(
                            [{'category_id': cat_id[0], 'business_id': business_id} for cat_id in category_ids]
                        )
                        conn.execute(category_business_statement)
                    else:
                        pass

                    # attributes table
                    if business['attributes'] is not None:
                        attributes = flatten_dict(business['attributes'])
                        attributes_statement = Insert(attributes_table).values(
                            [{'name': cat_name.strip()} for cat_name in attributes.keys()]
                        ).on_conflict_do_nothing()

                        conn.execute(attributes_statement)

                        attribute_ids_terms = conn.execute(select(attributes_table).where(attributes_table.c.name.in_(attributes.keys()))).fetchall()

                        # business attributes table
                        business_attributes_statement = Insert(business_attributes_table).values(
                            [{'attribute_id': entry[0], 'business_id': business_id, 'value': attributes[entry[1]].strip()} for entry in attribute_ids_terms]
                        )
                        conn.execute(business_attributes_statement)
                    else:
                        pass

                    # hours
                    if business['hours'] is not None:
                        # clean up days
                        days_dict = [(day, hours) for day, hours in business['hours'].items() if hours != '0:0-0:0']
                        hours_insert_statement = Insert(hours_table).values(
                            [{'business_id': business_id, 'day_id': day_id_dict[day], 'open_hours': hours} for day, hours in days_dict]
                        )
                        conn.execute(hours_insert_statement)
                    else:
                        pass


    def _load_users_json(self, users_file_path:Path, verbose:bool):
        """populates the users and user associated tables from user.json

        Args:
            users_file_path (Path): file Path object for user.json
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(users_file_path.name, False, verbose)

        users_table = Table('users', self.meta_data, autoload_with=self.engine)
        elite_table = Table('elite', self.meta_data, autoload_with=self.engine)

        def fix_split_elite(elite_str:str) -> list:
            """split the year elite string and fix 2020

            Args:
                elite_str (str): elite string years separated by a comma

            Returns:
                list[int]: years
            """
            elite = re.sub(r'20,20', r'2020', elite_str)
            return [int(year.strip()) for year in elite.split(',')]
        

        with self.engine.begin() as conn:
            for user in file_line_generator(users_file_path):

                

                users_statement = Insert(users_table).values(
                    user_id_str=user['user_id'],
                    name=user['name'],
                    review_count=user['review_count'],
                    yelping_since=user['yelping_since'],
                    useful=user['useful'],
                    funny=user['funny'],
                    cool=user['cool'],
                    fans=user['fans'],
                    average_stars=user['average_stars'],
                    compliment_hot=user['compliment_hot'],
                    compliment_more=user['compliment_more'],
                    compliment_profile=user['compliment_profile'],
                    compliment_cute=user['compliment_cute'],
                    compliment_list=user['compliment_list'],
                    compliment_note=user['compliment_note'],
                    compliment_plain=user['compliment_plain'],
                    compliment_cool=user['compliment_cool'],
                    compliment_funny=user['compliment_funny'],
                    compliment_writer=user['compliment_writer'],
                    compliment_photos=user['compliment_photos']
                )
                try:
                    user_id = conn.execute(users_statement).inserted_primary_key[0]
                except IntegrityError:
                    continue

                if user['elite'] is not None and user['elite'] != '':
                    years_elite = fix_split_elite(user['elite'])
                    if years_elite:

                        years_elite_statement = Insert(elite_table).values(
                            [{'user_id': user_id, 'year': year} for year in years_elite]
                        )

                        conn.execute(years_elite_statement)
                    else:
                        pass



    def _connect_users(self, users_file_path:Path, verbose:bool):
        """connect users based on the frineds category in json file
            count the friends and add them to the friend count column
            in the user table. if the user exist in the users table
            then add the user to the friends table.

        Args:
            users_file_path (Path): path to the users.json
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading('', True, verbose)

        users_table = Table('users', self.meta_data, autoload_with=self.engine)
        friends_table = Table('friends', self.meta_data, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for user in file_line_generator(users_file_path):


                get_user_id_statement = select([users_table.c.id]).where(users_table.c.user_id_str == user['user_id'])
                user_id = conn.execute(get_user_id_statement).fetchall()


                friends_list = [friend.strip() for friend in user['friends'].split(', ')]
                friend_count = len(friends_list)

                frined_count_statement = update(users_table).where(users_table.c.id == user_id[0][0]).values(friend_count=friend_count)
                conn.execute(frined_count_statement)

                get_friend_ids_statement = select([users_table.c.id]).where(users_table.c.user_id_str.in_(friends_list))
                friend_ids = conn.execute(get_friend_ids_statement).fetchall()
                
                if not friend_ids:
                    continue

                pair_friends_statement = Insert(friends_table).values(
                    [{'user1_id': user_id[0][0], 'user2_id': friend[0]} for friend in friend_ids]
                ).on_conflict_do_nothing()

                conn.execute(pair_friends_statement)


    def _load_review_json(self, review_file_path:Path, verbose:bool):
        """populate the reviews table from the reviews.json file

        Args:
            review_file_path (Path): path to review.json file
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(review_file_path.name, False, verbose)

        users_table = Table('users', self.meta_data, autoload_with=self.engine)
        business_table = Table('business', self.meta_data, autoload_with=self.engine)
        review_table = Table('reviews', self.meta_data, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for review in file_line_generator(review_file_path):



                user_id = self._get_add_user_id(conn, users_table, review['user_id'])
                business_id = self._get_add_business_id(conn, business_table, review['business_id'])
                

                review_statement = Insert(review_table).values(
                    review_id_str=review['review_id'],
                    user_id=user_id,
                    business_id=business_id,
                    stars=review['stars'],
                    date=review['date'],
                    text=review['text'],
                    useful=review['useful'],
                    funny=review['funny'],
                    cool=review['cool']
                ).on_conflict_do_nothing()

                conn.execute(review_statement)



    def _load_checkin_json(self, checkin_file_path:Path, verbose:bool):
        """populate the checkin table from the checkin.json file

        Args:
            checkin_file_path (Path): path to checkin.json file
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(checkin_file_path.name, False, verbose)

        checkin_table = Table('checkins', self.meta_data, autoload_with=self.engine)
        business_table = Table('business', self.meta_data, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for checkin in file_line_generator(checkin_file_path):

                checkin_dates = [date.strip() for date in checkin['date'].split(',')]

                business_id = self._get_add_business_id(conn, business_table, checkin['business_id'])

                checkin_statement = Insert(checkin_table).values(
                    [{'business_id': business_id, 'date': date} for date in checkin_dates]
                )

                conn.execute(checkin_statement)


    def _load_tip_json(self, tip_file_path:Path, verbose:bool):
        """populate the tips table from the tips.json file

        Args:
            tip_file_path (Path): path to tip.json file
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(tip_file_path.name, False, verbose)
        users_table = Table('users', self.meta_data, autoload_with=self.engine)
        business_table = Table('business', self.meta_data, autoload_with=self.engine)
        tips_table = Table('tips', self.meta_data, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for tip in file_line_generator(tip_file_path):
                
                user_id = self._get_add_user_id(conn, users_table, tip['user_id'])
                business_id = self._get_add_business_id(conn, business_table, tip['business_id'])

                tip_insert_statement = Insert(tips_table).values(
                    business_id=business_id,
                    user_id=user_id,
                    text=tip['text'],
                    date=tip['date'],
                    compliment_count=tip['compliment_count']
                ).on_conflict_do_nothing()

                conn.execute(tip_insert_statement)


    def _load_photos_json(self, photos_file_path:Path, verbose:bool):
        """populate the photos table from the photos.json file

        Args:
            photos_file_path (Path): path to photos.json file
            vebose (bool): print out progress using self.verbose_loading
        """

        self.verbose_loading(photos_file_path.name, False, verbose)

        business_table = Table('business', self.meta_data, autoload_with=self.engine)
        photos_table = Table('photos', self.meta_data, autoload_with=self.engine)

        with self.engine.begin() as conn:
            for photo in file_line_generator(photos_file_path):
                
                business_id = self._get_add_business_id(conn, business_table, photo['business_id'])

                photo_insert_statement = Insert(photos_table).values(
                    photo_id_str=photo['photo_id'],
                    business_id=business_id,
                    caption=photo['caption'],
                    label=photo['label']
                ).on_conflict_do_nothing()

                conn.execute(photo_insert_statement)


    def execute_sql_statement(self, statement:str, as_frame:bool=False):
        """function to execute a SQL statement

        Args:
            statement (str): the SQL Statement as a string
            as_frame (bool, optional): if True returns a pandas dataframe. Defaults to False.

        Returns:
            DataFrame | List[Row]: pandas dataframe or a list of row tuples
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(statement).fetchall()

        if as_frame:
            return pd.DataFrame(result)
        else:
            return result


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Creates SQLlite database for the Yelp Dataset from json files')

    parser.add_argument('config_file_path', type=str,
                        help='path to config.yaml file, see readme.md for more info')
    
    parser.add_argument('-np', '--no_photos', 
                        help="don't add photos.json",
                        action='store_false')
    
    parser.add_argument('-v', '--verbose', 
                        help='Show progress/steps',
                        action='store_true')
    args = parser.parse_args()

    ydb = YelpDataBase(config_file_path=args.config_file_path)
    ydb.create_full_database(verbose=args.verbose, include_photos=args.no_photos)