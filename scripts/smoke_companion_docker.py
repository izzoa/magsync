"""Offline container acceptance. Creates/removes only uniquely named test resources."""
import argparse
import json
import subprocess
import time
import uuid

parser = argparse.ArgumentParser()
parser.add_argument('--architecture', choices=['amd64', 'arm64'], required=True)
parser.add_argument('--stalled-health', action='store_true')
args = parser.parse_args()
architecture = args.architecture
service = f'magsync:companion-test-service-{architecture}'
daemon = f'magsync:companion-test-daemon-{architecture}'
name = 'magsync-smoke-' + uuid.uuid4().hex[:10]
volumes = [name + '-' + root for root in ('config', 'data', 'magazines', 'exports', 'view')]
view_client = None
mounts = [word for volume, root in zip(volumes, ('config', 'data', 'magazines', 'exports')) for word in ('-v', volume+':/'+root)]
containers = []
platform = ['--platform', 'linux/'+architecture]


def docker(*words, input=None, check=True):
    result = subprocess.run(['docker', *words], input=input, text=True, capture_output=True)
    if check and result.returncode:
        # Avoid dumping commands/stdin or credential-bearing outputs.
        raise RuntimeError(f'Docker {words[0]} failed ({result.returncode}): '+result.stderr[-1500:])
    return result


def run(image, *words, input=None, check=True):
    return docker('run', '--rm', '-i', *platform, *mounts, image, *words, input=input, check=check)


def api(container, path, token='', method='GET', body=None, headers=None):
    code = '''import json,sys,urllib.request,urllib.error
p=json.loads(sys.stdin.read());headers=p['headers'];headers['Authorization']='Bearer '+p['token']
if p['body'] is not None: headers['Content-Type']='application/json'
r=urllib.request.Request('http://127.0.0.1:8765'+p['path'],data=json.dumps(p['body']).encode() if p['body'] is not None else None,headers=headers,method=p['method'])
try:
 with urllib.request.urlopen(r,timeout=3) as response: print(json.dumps({'status':response.status,'body':response.read().decode()}))
except urllib.error.HTTPError as e: print(json.dumps({'status':e.code,'body':e.read().decode()}))
'''
    data = json.dumps(dict(path=path, token=token, method=method, body=body, headers=headers or {}))
    return json.loads(docker('exec', '-i', container, 'python', '-c', code, input=data).stdout)


def start(image, suffix):
    container = name+'-'+suffix
    containers.append(container)
    view_options = []
    if 'service' in image and view_client:
        view_options = ['-v', volumes[-1]+':/client-views/polyreader', '-e', 'MAGSYNC_TRUSTED_MOUNTS=1',
                        '-e', 'MAGSYNC_CLIENT_EXPORT_VIEWS='+json.dumps({view_client:'/client-views/polyreader'})]
    docker('run', '-d', '--name', container, *platform, *mounts, *view_options, image)
    for _ in range(100):
        result = docker('exec', container, 'python', '-m', 'magsync.companion.healthcheck', check=False)
        if result.returncode == 0:
            if 'service' in image:
                try:
                    if api(container, '/health/ready')['status'] != 200:
                        time.sleep(.1)
                        continue
                except RuntimeError:
                    time.sleep(.1)
                    continue
            return container
        time.sleep(.1)
    raise AssertionError('Runtime did not become ready')


try:
    for volume in volumes:
        docker('volume', 'create', volume)
    # Base extra isolation and no accidental listener.
    run(daemon, 'python', '-c', "import importlib.util; assert importlib.util.find_spec('fastapi') is None")
    running = start(daemon, 'daemon')
    check = docker('exec', running, 'python', '-c', "import socket; s=socket.socket(); assert s.connect_ex(('127.0.0.1',8765))!=0; assert __import__('os').getuid()!=0")
    docker('stop', '-t', '5', running)
    run(service, 'magsync', 'companion', 'init')
    credentials = json.loads(run(service, 'magsync', 'clients', 'create', 'Container fixture').stdout)
    view_client = credentials['client_id']
    # Initialize only our dedicated test volume for the documented non-root UID.
    docker('run', '--rm', *platform, '-v', volumes[-1]+':/view', '--user', '0', service, 'chown', '1000:1000', '/view')
    # Prepare verified fixture bytes without contacting any external source.
    seed = '''import hashlib,json
from pathlib import Path
from magsync.core.index import MagazineIndex
from magsync.core.models import DownloadStatus
from magsync.companion.store import Store
from magsync.companion.ownership import Ownership
from magsync.companion.exports import Exports
index=MagazineIndex();store=Store(index)
client=store.conn.execute("SELECT id FROM clients WHERE id!='local'").fetchone()[0]
scope=store.create_scope(client,{'external_id':'container-fixture','label':'Fixture'})
mag=index.get_or_create_magazine('Fixture','fixture')
index.add_issues(mag,[{'title':'Fixture - June 2025','year':2025,'month':6,'page_url':'https://freemagazines.top/container-fixture','limewire_url':None}])
issue=index.conn.execute('SELECT id FROM issues').fetchone()[0]
public=store.provider_issue(issue)
request=store.create_request(client,scope['id'],{'issue_id':public})
pdf=b'%PDF-1.7\\ncontainer fixture\\n%%EOF\\n';path=Path('/magazines/fixture.pdf');path.write_bytes(pdf)
index.update_download_status(issue,DownloadStatus.COMPLETE,str(path),len(pdf),hashlib.sha256(pdf).hexdigest())
with Ownership(store,Path('/magazines'),Path('/exports')) as owner:
 delivery=Exports(store,owner).publish(issue)[0]
 print(json.dumps(delivery))
index.close()
'''
    delivery = json.loads(run(service, 'python', '-', input=seed).stdout)
    running = start(service, 'service')
    assert api(running, '/v1/info')['status'] == 401
    info = json.loads(api(running, '/v1/info', credentials['token'])['body'])
    assert info['protocol_version'] == '1'
    assert 'trusted_client_mount' in info['capabilities']
    mounted = json.loads(api(running, '/v1/deliveries/'+delivery['id'], credentials['token'])['body'])
    assert mounted['transfer']['mount'] == delivery['id']+'.pdf'
    readonly_test = '''from pathlib import Path
import errno
files=list(Path('/inbox').glob('*.pdf'));assert len(files)==1 and files[0].read_bytes().startswith(b'%PDF')
try: Path('/inbox/forbidden').write_text('must fail')
except OSError as exc: assert exc.errno in (errno.EROFS, errno.EACCES)
else: raise AssertionError('Consumer mount is writable')
'''
    docker('run', '--rm', *platform, '-v', volumes[-1]+':/inbox:ro', service, 'python', '-c', readonly_test)
    assert json.loads(docker('inspect', running).stdout)[0]['HostConfig']['PortBindings'] == {}
    # A competing daemon must fail clearly without stealing the running owner.
    competitor = run(daemon, 'magsync', 'daemon', check=False)
    assert competitor.returncode != 0
    content = api(running, '/v1/deliveries/'+delivery['id']+'/content', credentials['token'])
    assert content['status'] == 200
    assert __import__('hashlib').sha256(content['body'].encode()).hexdigest() == delivery['sha256']
    receipt = dict(receipt_id='container-import', sha256=delivery['sha256'], size=delivery['size'])
    assert api(running, '/v1/deliveries/'+delivery['id']+'/ack', credentials['token'], 'PUT', receipt)['status'] == 200
    status = json.loads(docker('exec', running, 'magsync', 'companion', 'status').stdout)
    assert credentials['token'] not in json.dumps(status)
    docker('exec', running, 'magsync', 'clients', 'disable', credentials['client_id'])
    assert api(running, '/v1/info', credentials['token'])['status'] == 401
    docker('exec', running, 'magsync', 'clients', 'enable', credentials['client_id'])
    rotated = json.loads(docker('exec', running, 'magsync', 'clients', 'rotate', credentials['client_id'], '--revoke-old').stdout)
    assert api(running, '/v1/info', credentials['token'])['status'] == 401
    token = rotated['token']
    docker('stop', '-t', '5', running)
    running = start(service, 'replacement')
    replacement = json.loads(api(running, '/v1/info', token)['body'])
    assert replacement['instance_id'] == info['instance_id']
    assert replacement['recovery_epoch'] == info['recovery_epoch']
    persisted = json.loads(api(running, '/v1/deliveries/'+delivery['id'], token)['body'])
    assert persisted['receipt']['receipt_id'] == 'container-import'
    assert api(running, '/v1/deliveries/'+delivery['id']+'/ack', token, 'PUT', receipt)['status'] == 200
    if args.stalled_health:
        # The event loop stops touching the heartbeat, so Docker must go unhealthy.
        docker('kill', '--signal', 'STOP', running)
        deadline = time.monotonic()+100
        while time.monotonic() < deadline:
            if docker('inspect', '--format', '{{.State.Health.Status}}', running).stdout.strip() == 'unhealthy':
                break
            time.sleep(1)
        else:
            raise AssertionError('Stalled runtime did not become unhealthy')
        docker('kill', '--signal', 'CONT', running)
    docker('stop', '-t', '5', running)
    run(service, 'magsync', 'companion', 'purge', delivery['content_generation'])
    run(service, 'magsync', 'companion', 'recover')
    final = json.loads(run(service, 'magsync', 'companion', 'status').stdout)
    assert final['instance_id'] == info['instance_id']
    assert final['recovery_epoch'] != info['recovery_epoch']
    assert final['export_bytes'] == 0
    sizes = {}
    for image in (daemon, service):
        metadata = json.loads(docker('image', 'inspect', image).stdout)[0]
        assert metadata['Architecture'] == architecture
        assert metadata['Config']['User'] == 'magsync'
        sizes[image] = metadata['Size']
    assert sizes[daemon] < 200_000_000
    print(json.dumps({'architecture': architecture, 'checks': 'passed', 'stalled_health': args.stalled_health, 'trusted_mounts': True, 'image_bytes': sizes}))
finally:
    for container in containers:
        docker('kill', '--signal', 'CONT', container, check=False)
        docker('rm', '-f', '-v', container, check=False)
    for volume in volumes:
        docker('volume', 'rm', volume, check=False)
