from models import *


drop_tables()
create_tables()

delete_database()

insert_default_sources()
insert_default_channels()
insert_default_sinks()

insert_user_sources()
insert_user_channels()
insert_user_sinks()

print_config()