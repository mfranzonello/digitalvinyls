import re
import os
import sys

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.structure import SOUNDCLOUD_PLAY_URL, SOUNDCLOUD_SEARCH_URL, SOUNDCLOUD_TOKENS_FOLDER, write_json
from data.database import Neon

neon = Neon()
users_df = neon.get_users()
print('Choose the user you want to add SoundCloud to: ')
for i, user_s in users_df.iterrows():
    print(f'[{i+1}] {user_s["first_name"]} {user_s["last_name"]}')
user_s = users_df.loc[int(input('=> '))-1]

username = input(f'Enter the SoundCloud username for {user_s["first_name"]} {user_s["last_name"]}: ')
response = requests.get(f'{SOUNDCLOUD_PLAY_URL}/{username}')
user_uri = re.search('"uri":"https://api.soundcloud.com/users/(\d+)', response.text).group(1)

write_json(SOUNDCLOUD_TOKENS_FOLDER, user_s['user_email'], {'username': username, 'user_id': user_uri})

service_user_id = {'SoundCloud': user_uri}
neon.update_user(user_s['user_id'], service_user_ids=service_user_id)