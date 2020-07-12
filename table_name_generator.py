import uuid
import base64

# Let's you generate a random string you can use as a table name or anything

def generate_table_name(tasktype, taskname):
    generated_name = f"{tasktype}_{taskname}_"
    gen_uuid = uuid.uuid4()
    gen_uuid = str(gen_uuid)[:8]
    generated_name = generated_name + str(gen_uuid)
    return generated_name


def generate_uuid(input_str):
    source_uuid = base64.b64encode(bytes(input_str, "utf-8"))
    return str(source_uuid.decode("utf-8"))


def generate_temp_filename():
    gen_uuid = uuid.uuid4()
    gen_uuid = str(gen_uuid)[:8]
    generated_name = "temp_" + str(gen_uuid)
    return generated_name


if __name__ == "__main__":
    tasktype = "test1" 
    taskname = "test 2"
    generate_table_name(tasktype, taskname)
    
