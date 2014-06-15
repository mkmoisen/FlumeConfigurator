import MySQLdb
import peewee
from peewee import *
import sys

db = MySQLDatabase('flume', user='root',passwd='mysql')

class BaseModel(Model):
    class Meta:
        database = db


class Item(BaseModel):
    category = CharField()
    type = CharField()

class ItemAttribute(BaseModel):
    item = ForeignKeyField(Item)
    name = CharField()
    description = CharField()

class ItemAttributeOption(BaseModel):
    item_attribute = ForeignKeyField(ItemAttribute)
    option = CharField()
    data_type = CharField()

class Source(BaseModel):
    type = CharField()

class SourceAttribute(BaseModel):
    source = ForeignKeyField(Source)
    name = CharField()
    description = CharField()

class SourceAttributeOption(BaseModel):
    source_attribute = ForeignKeyField(SourceAttribute)
    option = CharField()
    data_type = CharField()


class Channel(BaseModel):
    type = CharField()

class ChannelAttribute(BaseModel):
    channel = ForeignKeyField(Channel)
    name = CharField()
    description = CharField()

class ChannelAttributeOption(BaseModel):
    channel_attribute = ForeignKeyField(ChannelAttribute)
    option = CharField()
    data_type = CharField()

class Sink(BaseModel):
    type = CharField()

class SinkAttribute(BaseModel):
    sink = ForeignKeyField(Sink)
    name = CharField()
    description = CharField()

class SinkAttributeOption(BaseModel):
    sink_attribute = ForeignKeyField(SinkAttribute)
    option = CharField()
    data_type = CharField()

class Project(BaseModel):
    name = CharField()

class Agent(BaseModel):
    project = ForeignKeyField(Project)
    name = CharField()

class UserSource(BaseModel):
    agent = ForeignKeyField(Agent)
    source = ForeignKeyField(Source)
    name = CharField()

class UserSourceAttribute(BaseModel):
    user_source = ForeignKeyField(UserSource)
    source_attribute = ForeignKeyField(SourceAttribute)
    value = CharField()


class UserChannel(BaseModel):
    agent = ForeignKeyField(Agent)
    channel = ForeignKeyField(Channel)
    name = CharField()

class UserChannelAttribute(BaseModel):
    user_channel = ForeignKeyField(UserChannel)
    channel_attribute = ForeignKeyField(ChannelAttribute)
    value = CharField()

class UserSourceChannel(BaseModel):
    user_source = ForeignKeyField(UserSource)
    user_channel = ForeignKeyField(UserChannel)

class UserSink(BaseModel):
    agent = ForeignKeyField(Agent)
    sink = ForeignKeyField(Sink)
    name = CharField()

class UserSinkAttribute(BaseModel):
    user_sink = ForeignKeyField(UserSink)
    sink_attribute = ForeignKeyField(SinkAttribute)
    value = CharField()

class UserChannelSink(BaseModel):
    user_channel = ForeignKeyField(UserChannel)
    user_sink = ForeignKeyField(UserSink, unique=True)



def drop_tables():
    UserSourceChannel.drop_table(True)
    UserChannelSink.drop_table(True)
    UserChannelAttribute.drop_table(True)
    UserChannel.drop_table(True)
    UserSourceAttribute.drop_table(True)
    UserSource.drop_table(True)
    UserSinkAttribute.drop_table(True)
    UserSink.drop_table(True)
    Agent.drop_table(True)
    Project.drop_table(True)
    SourceAttributeOption.drop_table(True)
    SourceAttribute.drop_table(True)
    Source.drop_table(True)
    ChannelAttributeOption.drop_table(True)
    ChannelAttribute.drop_table(True)
    Channel.drop_table(True)
    SinkAttributeOption.drop_table(True)
    SinkAttribute.drop_table(True)
    Sink.drop_table(True)

def create_tables():
    Source.create_table(True)
    SourceAttribute.create_table(True)
    SourceAttributeOption.create_table(True)
    Channel.create_table(True)
    ChannelAttribute.create_table(True)
    ChannelAttributeOption.create_table(True)
    Sink.create_table(True)
    SinkAttribute.create_table(True)
    SinkAttributeOption.create_table(True)
    Project.create_table(True)
    Agent.create_table(True)
    UserSource.create_table(True)
    UserSourceAttribute.create_table(True)
    UserChannel.create_table(True)
    UserChannelAttribute.create_table(True)
    UserSink.create_table(True)
    UserSinkAttribute.create_table(True)
    UserSourceChannel.create_table(True)
    UserChannelSink.create_table(True)


def delete_database():
    UserChannelAttribute.delete().execute()
    UserChannel.delete().execute()
    UserSourceAttribute.delete().execute()
    UserSource.delete().execute()
    UserSinkAttribute.delete().execute()
    UserSink.delete().execute()
    Agent.delete().execute()
    Project.delete().execute()
    SourceAttributeOption.delete().execute()
    SourceAttribute.delete().execute()
    Source.delete().execute()
    ChannelAttributeOption.delete().execute()
    ChannelAttribute.delete().execute()
    Channel.delete().execute()

def insert_default_sources():
    spool = Source.create(type='spool')
    spoolDir = SourceAttribute.create(source=spool, name="spoolDir", description="The directory from which to read files from")
    spoolDirOption = SourceAttributeOption.create(source_attribute=spoolDir, option="user_supplied", data_type="string")
    spool_delete_policy = SourceAttribute.create(source=spool, name="deletePolicy", description="When to delete completed files: never or immediate")
    delete_policy_never = SourceAttributeOption.create(source_attribute=spool_delete_policy, option="never", data_type="string")
    delete_policy_immediate = SourceAttributeOption.create(source_attribute=spool_delete_policy, option="immediate", data_type="string")

def insert_default_channels():
    memory = Channel.create(type='memory')
    memory_capacity = ChannelAttribute.create(channel=memory, name="capacity", description="The maximum number of events stored in the channel")
    memory_capacity_option = ChannelAttributeOption.create(channel_attribute=memory_capacity, option="user_supplied", data_type="int")
    memory_transaction_capacity = ChannelAttribute.create(channel=memory, name="transactionCapacity", description=" 	The maximum number of events the channel will take from a source or give to a sink per transaction")
    memory_transaction_capacity_option = ChannelAttributeOption.create(channel_attribute=memory_transaction_capacity, option="user_supplied", data_type="int")
    
    file = Channel.create(type='file')
    file_checkpoint_dir = ChannelAttribute.create(channel=file, name="checkpointDir", description="The directory where checkpoint file will be stored")
    file_checkpoint_dir_option = ChannelAttributeOption.create(channel_attribute=file_checkpoint_dir, option="user_supplied", data_type="string")
    file_data_dirs = ChannelAttribute.create(channel=file, name="dataDirs", description="Comma separated list of directories for storing log files. Using multiple directories on separate disks can improve file channel peformance")
    file_data_dirs_option = ChannelAttributeOption.create(channel_attribute=file_data_dirs, option="user_supplied", data_type="string")

def insert_default_sinks():
    hdfs = Sink.create(type='hdfs')
    hdfs_path = SinkAttribute.create(sink=hdfs, name="path", description="HDFS directory path (eg hdfs://namenode/flume/webdata/)")
    hdfs_path_option = SinkAttributeOption.create(sink_attribute=hdfs_path, option="user_supplied", data_type="string")

    logger = Sink.create(type="logger")
    logger_hostname = SinkAttribute.create(sink=logger, name="hostname", description="")
    logger_hostname_option = SinkAttributeOption.create(sink_attribute=logger_hostname, option="user_supplied", data_type="string")
    logger_port = SinkAttribute.create(sink=logger, name="port", description="")
    logger_port_option = SinkAttributeOption.create(sink_attribute=logger_port, option="user_supplied", data_type="int")


def insert_user_sources():
    project = Project.create(name="test")
    agent = Agent.create(project=project, name="agent")
    source = Source.select().where(Source.type=="spool")
    user_source = UserSource.create(agent=agent, source=source, name="s1")
    spool_dir_attribute = SourceAttribute.select().where(SourceAttribute.source==source, SourceAttribute.name=="spoolDir")
    spool_dir_value = UserSourceAttribute.create(user_source=user_source,source_attribute=spool_dir_attribute, value="/home/flume")
    delete_policy_attribute = SourceAttribute.select().where(SourceAttribute.source==source, SourceAttribute.name=="deletePolicy")
    delete_policy_value = UserSourceAttribute.create(user_source=user_source, source_attribute=delete_policy_attribute, value="immediate")

def insert_user_channels():
    project = Project.select().where(Project.name == "test")
    agent = Agent.select().where(Agent.project == project, Agent.name == "agent")
    channel = Channel.select().where(Channel.type == "memory")
    user_channel = UserChannel.create(agent=agent, channel=channel, name="c1")
    memory_capacity = ChannelAttribute.select().where(ChannelAttribute.channel == channel, ChannelAttribute.name == "capacity")
    memory_capacity_value = UserChannelAttribute.create(user_channel=user_channel, channel_attribute=memory_capacity, value=100)
    memory_transaction_capacity = ChannelAttribute.select().where(ChannelAttribute.channel == channel, ChannelAttribute.name == "transactionCapacity")
    memory_transaction_capacity_value = UserChannelAttribute.create(user_channel=user_channel, channel_attribute=memory_transaction_capacity, value=100)
    user_source = UserSource.select().where(UserSource.agent == agent, UserSource.name == "s1")
    user_source_channel = UserSourceChannel.create(user_source=user_source, user_channel=user_channel)
    
    channel_f = Channel.select().where(Channel.type=="file")
    user_channel_f = UserChannel.create(agent=agent, channel=channel_f, name="c2")
    checkpoint_dir = ChannelAttribute.select().where(ChannelAttribute.channel == channel_f, ChannelAttribute.name == "checkpointDir")
    checkpoint_dir_value = UserChannelAttribute.create(user_channel=user_channel_f, channel_attribute=checkpoint_dir, value="/home/flume/checkpoint")
    data_dirs = ChannelAttribute.select().where(ChannelAttribute.channel == channel_f, ChannelAttribute.name == "dataDirs")
    data_dirs_Value = UserChannelAttribute.create(user_channel=user_channel_f, channel_attribute=data_dirs, value="/home/flume/data1,/home/flume/data2")

def insert_user_sinks():
    project = Project.select().where(Project.name == "test")
    agent = Agent.select().where(Agent.project == project, Agent.name == "agent")
    hdfs = Sink.select().where(Sink.type=="hdfs")
    user_hdfs = UserSink.create(agent=agent, sink=hdfs, name="k1")
    hdfs_path = SinkAttribute.select().where(SinkAttribute.sink == hdfs, SinkAttribute.name == "path")
    hdfs_path_value = UserSinkAttribute.create(user_sink=user_hdfs, sink_attribute=hdfs_path, value="/user/root/flume")

    logger = Sink.select().where(Sink.type=="logger")
    user_logger = UserSink.create(agent=agent, sink=logger, name="k2")
    logger_hostname = SinkAttribute.select().where(SinkAttribute.sink == logger, SinkAttribute.name == "hostname")
    logger_hostname_value = UserSinkAttribute.create(user_sink=user_logger, sink_attribute=logger_hostname, value="www.matthewmoisen.com")
    logger_port = SinkAttribute.select().where(SinkAttribute.sink == logger, SinkAttribute.name == "port")
    logger_port_value = UserSinkAttribute.create(user_sink=user_logger, sink_attribute=logger_port, value="12345")

    channel_memory = Channel.select().where(Channel.type=="memory")
    hdfs_memory = UserChannelSink.create(user_channel=channel_memory, user_sink=user_hdfs)
    channel_file = Channel.select().where(Channel.type=="file")
    logger_file = UserChannelSink.create(user_channel=channel_file, user_sink=user_logger)



def print_config():
    for agent in Agent.select().join(Project).where(Project.name=="test"):
        print "This Flume project '%s' has been generated by Matthew Moisen's Flume Configurator" % (agent.project.name)
        print("")
        sys.stdout.write(agent.name + ".sources=")
        for user_source in UserSource.select().join(Agent).where(Agent.id==agent):
            sys.stdout.write(user_source.name + " ")
        print("")
        sys.stdout.write(agent.name + ".channels=")
        for user_channel in UserChannel.select().join(Agent).where(Agent.id==agent):
            sys.stdout.write(user_channel.name + " ")
        print("")
        print("")
        for user_source in UserSource.select().join(Source).switch(UserSource).join(Agent).where(UserSource.source == Source.id, Agent.id==agent):
            print user_source.agent.name + "." + user_source.name + ".type=" + user_source.source.type
            for usa in UserSourceAttribute.select().join(SourceAttribute).switch(UserSourceAttribute).join(UserSource).where(UserSource.id==user_source):
                print '%s.%s.%s=%s' % (usa.user_source.agent.name, usa.user_source.name, usa.source_attribute.name, usa.value)
            sys.stdout.write(user_source.agent.name + '.' + user_source.name + '.channels=')
            for usc in UserSourceChannel.select().join(UserSource).switch(UserSourceChannel).join(UserChannel).where(UserSource.id == user_source):
                sys.stdout.write(usc.user_channel.name + ' ')
        print("")
        print("")
        for user_channel in UserChannel.select().join(Channel).switch(UserChannel).join(Agent).where(Agent.id==agent):
            print agent.name + '.' + user_channel.name + '.type=' + user_channel.channel.type
            for uca in UserChannelAttribute.select().join(ChannelAttribute).switch(UserChannelAttribute).join(UserChannel).where(UserChannel.id==user_channel):
                print uca.user_channel.agent.name + "." + uca.user_channel.name + "." + uca.channel_attribute.name + "=" + uca.value
            print("")
        
        for user_sink in UserSink.select().join(Sink).switch(UserSink).join(Agent).where(Agent.id==agent):
            print agent.name + '.' + user_sink.name + '.type=' + user_sink.sink.type
            user_channel_sink = UserChannelSink.select().join(UserChannel).where(UserChannelSink.user_sink==user_sink).first()
            print "%s.%s.channel=%s" % (agent.name, user_sink.name, user_channel_sink.user_channel.name)
            for usa in UserSinkAttribute.select().join(SinkAttribute).switch(UserSinkAttribute).join(UserSink).where(UserSink.id==user_sink):
                print usa.user_sink.agent.name + '.' + usa.user_sink.name + '.' + usa.sink_attribute.name + "=" + usa.value
            print("")


 
#SELECT t1.`id`, t1.`user_channel_id`, t1.`user_sink_id` FROM `userchannelsink` AS t1 INNER JOIN `userchannel` AS t2 ON (t1.`user_channel_id` = t2.`id`) WHERE (t1.`user_sink_id` = 1)