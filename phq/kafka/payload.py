from collections import namedtuple
import json
import logging
from typing import Dict, List, Any, Union

import rfc3339
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Message:
    id: str
    payload: Dict[str, Any]
    ref: Union[Dict[str, Any], List[Dict[str, Any]]]
    topic: str = None


def _long_hist(hist, depth=0, max_depth=100):
    if not hist:
        return False

    depth += 1
    if depth >= max_depth:
        return True

    if not hist['refs']:
        return False

    refs = hist['refs']

    # Some old messages have refs set to a single
    # ref dict, not a list of ref dicts.
    # If we find this pretend it's a long hist so
    # we truncate it - easier than trying to fix properly.
    # TODO: Remove at some point in the future when this
    #       bad data is cleaned out.
    if isinstance(refs, (dict,)):
        return True

    for ref in refs:
        if _long_hist(ref['hist'], depth, max_depth):
            return True

    return False


def _get_kafka_ref(message, hist=None):
    return {
        'topic': message.topic(),
        'partition': message.partition(),
        'offset': message.offset(),
        'key': message.key().decode('utf-8'),
        'hist': hist,
    }


def format_kafka_ref(ref):
    return '{}:{}:{}:{}'.format(ref['topic'], ref['partition'], ref['offset'], ref['key'])


def unpack_kafka_payload(payload):
    value = payload.value()
    if not value:
        return None
    data = json.loads(value.decode('utf-8'))
    item = data['item']
    hist = data.get('hist', None)

    long_hist_truncated = False
    if _long_hist(hist):
        long_hist_truncated = True
        hist = None

    ref = _get_kafka_ref(payload, hist)

    if long_hist_truncated:
        log.warning('[%(ref)s] Message has a very long history. Truncated.',
                    {'ref': format_kafka_ref(ref)})

    return item, ref


def pack_kafka_payload(svc, item, refs):
    payload = {
        'item': item,
        'hist': {
            'svc': svc,
            'dt': rfc3339.datetimetostr(rfc3339.now()),
            'refs': refs or []
        }
    }
    return json.dumps(payload)
