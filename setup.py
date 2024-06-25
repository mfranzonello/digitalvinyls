''' Ensure database and listener are good to go '''

from data.database import Neon
from music.listeners import User

def set_up_database(drop_tables=False, drop_views=False, create_tables=False, create_views=False, materialize=False):
    neon = Neon()
    neon.connect()
    if drop_tables:
        neon.drop_tables()
    if create_tables:
        neon.create_tables()
    if drop_views:
        neon.drop_views()
    if create_views:
        neon.create_views()
    if materialize:
        neon.materialize_views()
    return neon

def set_up_user(neon, user_id):
    user_s = neon.get_user(user_id)
    user = User(user_id, user_s['first_name'], user_s['last_name'])
    user.add_services(user_s['service_user_ids'])
    return user

def set_up_users(neon):
    users = [set_up_user(neon, user_id) for user_id in neon.get_user_ids()]
    return users

def is_updatable(df):
    return (df is not None) and (not df.empty)

def main():
    set_up_database(drop_tables=False, create_tables=True, drop_views=True, create_views=True, materialize=True)

if __name__ == '__main__':
    main()