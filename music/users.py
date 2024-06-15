class User:
    def __init__(self, user_id, first_name, last_name):
        self.user_id = user_id
        self.first_name = first_name
        self.last_name = last_name
        self.user_ids = {}
        self.image_src = None
        
    def add_services(self, service_user_ids):
        self.user_ids = service_user_ids
        
    def has_service(self, service_name):
        return self.user_ids.get(service_name) is not None

    def get_user_id(self, service_id):
        return self.user_ids[service_id]

    def add_picture(self, image_src):
        self.image_src = image_src