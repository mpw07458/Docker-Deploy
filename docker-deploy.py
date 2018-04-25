import docker
_docker_client_id=''

def __init__():
    _docker_client_id=_get_docker_client()

def _get_docker_client():
    client = docker.from_env()
    return client

def deploy_container(docker_client, contain_name, contain_cmd,cmd_message ):
    '''
    # deploy container
    :param contain_name:
    :param contain_cmd:
    :param cmd_message:
    :return:
    '''
    print docker_client.containers.run("alpine", ["echo", "hello", "world"])

def deploy_detached_container(docker_client, contain_name, detach):
    '''
    deploy container in background
    :param docker_client:
    :param contain_name:
    :param detach:
    :return:
    '''
    container = docker_client.containers.run("bfirsh/reticulate-splines", detach=True)
    print container.id
    return container.id

def list_all_containers(docker_client):
    '''
    # list all containers
    :param docker_client:
    :return:
    '''
    for container in docker_client.containers.list():
        print container.id

def stop_all_containers(docker_client):
    '''
    stop all containers
    :param docker_client:
    :return:
    '''
    for container in docker_client.containers.list():
        container.stop()

#get container logs
def get_container_logs(docker_client):
    container = docker_client.containers.get('f1064a8a4c82')
    print container.logs()

# get image list
def get_image_list(docker_client):
    for image in docker_client.images.list():
        print image.id

#  pull an image
def pull_docker_image(docker_client):
    image = docker_client.images.pull("alpine")
    print image.id
    return image.id

# commit container
def commit_docker_container(docker_client):
    container = docker_client.containers.run("alpine", ["touch", "/helloworld"], detach=True)
    container.wait()
    image = container.commit("helloworld")
    print image.id
    return image.id



