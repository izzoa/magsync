from __future__ import annotations

import json
import shutil
import sqlite3

import pytest

from magsync.companion.exports import Exports
from magsync.companion.journal import Journal
from magsync.companion.ownership import Ownership
from magsync.companion.protocol import ProtocolError
from magsync.companion.store import Store
from magsync.core.index import MagazineIndex
from test_companion_store import store
from test_companion_exports import delivery_setup, PDF


def test_coordinated_backup_restore_requires_resync_preserves_import(delivery_setup,tmp_path):
    store,owner,exports,journal,client,scope,request,internal,original=delivery_setup
    delivery=exports.publish(internal)[0]
    backup=tmp_path/'backup'
    backup.mkdir()
    db=sqlite3.connect(backup/'index.db')
    store.conn.backup(db);db.close()
    shutil.copyfile(str(store.index.db_path)+'.identity.json', backup/'index.db.identity.json')
    shutil.copytree(owner.exports,backup/'exports')
    imported=tmp_path/'consumer-managed.pdf';imported.write_bytes(PDF)
    receipt={'receipt_id':'managed-copy','sha256':delivery['sha256'],'size':delivery['size']}
    journal.acknowledge(client['client_id'],delivery['id'],receipt)
    cursor=journal.events(client['client_id'])['cursor']
    instance=store.identity()['instance_id']
    # Stop every writer before restoring the paired DB/files. Paths are kept
    # consistent just as documented; restore does not touch consumer storage.
    output,exports_root=owner.output,owner.exports
    owner.release()
    store.index.close()
    shutil.copyfile(backup/'index.db',store.index.db_path)
    for suffix in ('-wal','-shm'):
        __import__('pathlib').Path(str(store.index.db_path)+suffix).unlink(missing_ok=True)
    shutil.rmtree(exports_root);shutil.copytree(backup/'exports',exports_root)
    index=MagazineIndex(store.index.db_path)
    restored=Store(index)
    with Ownership(restored,output,exports_root) as new_owner:
        reconciler=Journal(restored,Exports(restored,new_owner))
        reconciler.rotate_epoch()
        assert restored.identity()['instance_id']==instance
        with pytest.raises(ProtocolError) as error:
            reconciler.events(client['client_id'],cursor)
        assert error.value.code=='resync_required'
        snapshot=reconciler.snapshot(client['client_id'])
        recovered=[i['resource'] for i in snapshot['items'] if i['kind']=='delivery'][0]
        assert recovered['id']==delivery['id'] and recovered['receipt'] is None
        assert imported.read_bytes()==PDF
        assert reconciler.acknowledge(client['client_id'],delivery['id'],receipt)['receipt_id']=='managed-copy'
    index.close()
