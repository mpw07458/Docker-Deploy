import functools
import os
import random
import re
import socket
import tarfile
import tempfile
import time

import docker
from os import os.path
import pytest
import six
from docker.errors import InvalidArgument
from docker.types.services import ServiceMode

TEST_API_VERSION = '2.3.2'


class Docker_Deploy(object):

    def __init__(self):
        pass

    @classmethod
    def setUpClass(self, cls):
        client = docker.from_env(version=TEST_API_VERSION)
        self.force_leave_swarm(client)
        client.swarm.init('127.0.0.1', listen_addr=self.swarm_listen_addr())

    @classmethod
    def tearDownClass(self, cls):
        self.force_leave_swarm(docker.from_env(version=TEST_API_VERSION))

    def test_create(self):
        client = docker.from_env(version=TEST_API_VERSION)
        name = self.random_name()
        service = client.services.create(
            # create arguments
            name=name,
            labels={'foo': 'bar'},
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300",
            container_labels={'container': 'label'}
        )
        assert service.name == name
        assert service.attrs['Spec']['Labels']['foo'] == 'bar'
        container_spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert "alpine" in container_spec['Image']
        assert container_spec['Labels'] == {'container': 'label'}

    def test_create_with_network(self):
        client = docker.from_env(version=TEST_API_VERSION)
        name = self.random_name()
        network = client.networks.create(
            self.random_name(), driver='overlay'
        )
        service = client.services.create(
            # create arguments
            name=name,
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300",
            networks=[network.id]
        )
        assert 'Networks' in service.attrs['Spec']['TaskTemplate']
        networks = service.attrs['Spec']['TaskTemplate']['Networks']
        assert len(networks) == 1
        assert networks[0]['Target'] == network.id

    def test_get(self):
        client = docker.from_env(version=TEST_API_VERSION)
        name = self.random_name()
        service = client.services.create(
            name=name,
            image="alpine",
            command="sleep 300"
        )
        service = client.services.get(service.id)
        assert service.name == name

    def test_list_remove(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            name=self.random_name(),
            image="alpine",
            command="sleep 300"
        )
        assert service in client.services.list()
        service.remove()
        assert service not in client.services.list()

    def test_tasks(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service1 = client.services.create(
            name=self.random_name(),
            image="alpine",
            command="sleep 300"
        )
        service2 = client.services.create(
            name=self.random_name(),
            image="alpine",
            command="sleep 300"
        )
        tasks = []
        while len(tasks) == 0:
            tasks = service1.tasks()
        assert len(tasks) == 1
        assert tasks[0]['ServiceID'] == service1.id

        tasks = []
        while len(tasks) == 0:
            tasks = service2.tasks()
        assert len(tasks) == 1
        assert tasks[0]['ServiceID'] == service2.id

    def test_update(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        service.update(
            # create argument
            name=service.name,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        container_spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert container_spec['Command'] == ["sleep", "600"]

    def test_update_retains_service_labels(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            labels={'service.label': 'SampleLabel'},
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        service.update(
            # create argument
            name=service.name,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        labels = service.attrs['Spec']['Labels']
        assert labels == {'service.label': 'SampleLabel'}

    def test_update_retains_container_labels(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300",
            container_labels={'container.label': 'SampleLabel'}
        )
        service.update(
            # create argument
            name=service.name,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        container_spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert container_spec['Labels'] == {'container.label': 'SampleLabel'}

    def test_update_remove_service_labels(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            labels={'service.label': 'SampleLabel'},
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        service.update(
            # create argument
            name=service.name,
            labels={},
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        assert not service.attrs['Spec'].get('Labels')

    @pytest.mark.xfail(reason='Flaky test')
    def test_update_retains_networks(self):
        client = docker.from_env(version=TEST_API_VERSION)
        network_name = self.random_name()
        network = client.networks.create(
            network_name, driver='overlay'
        )
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            networks=[network.id],
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        service.reload()
        service.update(
            # create argument
            name=service.name,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        networks = service.attrs['Spec']['TaskTemplate']['Networks']
        assert networks == [{'Target': network.id}]

    def test_scale_service(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        tasks = []
        while len(tasks) == 0:
            tasks = service.tasks()
        assert len(tasks) == 1
        service.update(
            mode=docker.types.ServiceMode('replicated', replicas=2),
        )
        while len(tasks) == 1:
            tasks = service.tasks()
        assert len(tasks) >= 2
        # check that the container spec is not overridden with None
        service.reload()
        spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert spec.get('Command') == ['sleep', '300']

    def test_scale_method_service(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300",
        )
        tasks = []
        while len(tasks) == 0:
            tasks = service.tasks()
        assert len(tasks) == 1
        service.scale(2)
        while len(tasks) == 1:
            tasks = service.tasks()
        assert len(tasks) >= 2
        # check that the container spec is not overridden with None
        service.reload()
        spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert spec.get('Command') == ['sleep', '300']

    def test_scale_method_global_service(self):
        client = docker.from_env(version=TEST_API_VERSION)
        mode = ServiceMode('global')
        service = client.services.create(
            name=self.random_name(),
            image="alpine",
            command="sleep 300",
            mode=mode
        )
        tasks = []
        while len(tasks) == 0:
            tasks = service.tasks()
        assert len(tasks) == 1
        with pytest.raises(InvalidArgument):
            service.scale(2)

        assert len(tasks) == 1
        service.reload()
        spec = service.attrs['Spec']['TaskTemplate']['ContainerSpec']
        assert spec.get('Command') == ['sleep', '300']

    def test_force_update_service(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        initial_version = service.version
        assert service.update(
            # create argument
            name=service.name,
            # task template argument
            force_update=10,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        assert service.version > initial_version

    def test_force_update_service_using_bool(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        initial_version = service.version
        assert service.update(
            # create argument
            name=service.name,
            # task template argument
            force_update=True,
            # ContainerSpec argument
            command="sleep 600"
        )
        service.reload()
        assert service.version > initial_version

    def test_force_update_service_using_shorthand_method(self):
        client = docker.from_env(version=TEST_API_VERSION)
        service = client.services.create(
            # create arguments
            name=self.random_name(),
            # ContainerSpec arguments
            image="alpine",
            command="sleep 300"
        )
        initial_version = service.version
        assert service.force_update()
        service.reload()
        assert service.version > initial_version

    def make_tree(dirs, files):
        base = tempfile.mkdtemp()

        for path in dirs:
            os.makedirs(os.path.join(base, path))

        for path in files:
            with open(os.path.join(base, path), 'w') as f:
                f.write("content")

        return base

    def simple_tar(path):
        f = tempfile.NamedTemporaryFile()
        t = tarfile.open(mode='w', fileobj=f)

        abs_path = os.path.abspath(path)
        t.add(abs_path, arcname=os.path.basename(path), recursive=False)

        t.close()
        f.seek(0)
        return f

    def untar_file(tardata, filename):
        with tarfile.open(mode='r', fileobj=tardata) as t:
            f = t.extractfile(filename)
            result = f.read()
            f.close()
        return result

    def requires_api_version(version):
        test_version = os.environ.get(
            'DOCKER_TEST_API_VERSION', docker.constants.DEFAULT_DOCKER_API_VERSION
        )

        return pytest.mark.skipif(
            docker.utils.version_lt(test_version, version),
            reason="API version is too low (< {0})".format(version)
        )

    def requires_experimental(until=None):
        test_version = os.environ.get(
            'DOCKER_TEST_API_VERSION', docker.constants.DEFAULT_DOCKER_API_VERSION
        )

        def req_exp(f):
            @functools.wraps(f)
            def wrapped(self, *args, **kwargs):
                if not self.client.info()['ExperimentalBuild']:
                    pytest.skip('Feature requires Docker Engine experimental mode')
                return f(self, *args, **kwargs)

            if until and docker.utils.version_gte(test_version, until):
                return f
            return wrapped

        return req_exp

    def wait_on_condition(condition, delay=0.1, timeout=40):
        start_time = time.time()
        while not condition():
            if time.time() - start_time > timeout:
                raise AssertionError("Timeout: %s" % condition)
            time.sleep(delay)

    def random_name(self):
        return u'dockerpytest_{0:x}'.format(random.getrandbits(64))

    def force_leave_swarm(client):
        """Actually force leave a Swarm. There seems to be a bug in Swarm that
        occasionally throws "context deadline exceeded" errors when leaving."""
        while True:
            try:
                if isinstance(client, docker.DockerClient):
                    return client.swarm.leave(force=True)
                return client.leave_swarm(force=True)  # elif APIClient
            except docker.errors.APIError as e:
                if e.explanation == "context deadline exceeded":
                    continue
                else:
                    return

    def swarm_listen_addr(self):
        return '0.0.0.0:{0}'.format(random.randrange(10000, 25000))

    def assert_cat_socket_detached_with_keys(sock, inputs):
        if six.PY3 and hasattr(sock, '_sock'):
            sock = sock._sock

        for i in inputs:
            sock.sendall(i)
            time.sleep(0.5)

        # If we're using a Unix socket, the sock.send call will fail with a
        # BrokenPipeError ; INET sockets will just stop receiving / sending data
        # but will not raise an error
        if getattr(sock, 'family', -9) == getattr(socket, 'AF_UNIX', -1):
            with pytest.raises(socket.error):
                sock.sendall(b'make sure the socket is closed\n')
        else:
            sock.sendall(b"make sure the socket is closed\n")
            assert sock.recv(32) == b''

    def ctrl_with(char):
        if re.match('[a-z]', char):
            return chr(ord(char) - ord('a') + 1).encode('ascii')
        else:
            raise (Exception('char must be [a-z]'))
