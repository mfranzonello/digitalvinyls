''' Ensure database and listener are good to go '''

from data.database import Neon
from music.users import User

def set_up_database(drop_tables=False, drop_views=False):
    neon = Neon()
    neon.connect()
    if drop_tables:
        neon.drop_tables()
    neon.create_tables()
    if drop_views:
        neon.drop_views()
    neon.create_views()
    return neon

def set_up_user(neon, user_id):
    user_s = neon.get_user(user_id)
    user = User(user_id, user_s['first_name'], user_s['last_name'])
    user.add_services(user_s['service_user_ids'])
    return user

if __name__ == '__main__':
    set_up_database(drop_views=True)
    
    quit()