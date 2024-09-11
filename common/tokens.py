
import os

from .structure import read_json, write_json

def save_token(folder_name, user_id, token_details):
    write_json(folder_name, user_id, token_details)

def get_token(folder_name, user_id):
    return read_json(folder_name, user_id)
    
def update_profile(folder_name, profile_email, profile_user_info):
    # read in profile data
    profiles = read_json(folder_name, 'profiles')
    
    # check if profile already exists
    existing = next(filter(lambda x: x['email'] == profile_email, profiles), None)
    if not existing:
        # if not, add it in
        p = len(profiles)
        profiles.append({'email': profile_email})
        
    else:
        # get index of existing profile
        p = profiles.index(existing)
        
    # udpate profile data
    if isinstance(profile_user_info, dict):
        # update with new info
        profiles[p].update(profile_user_info)
    elif isinstance(profile_user_info, str):
        # remove old data
        profiles[p].pop(profile_user_info)
    
    # save file
    write_json(folder_name, 'profiles', profiles)
    
