"""Offline drying telemetry and action tests with minimal HA/transport stubs."""
import asyncio
import importlib
from enum import IntFlag
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch


def module(name, **attrs):
    result = ModuleType(name)
    result.__dict__.update(attrs)
    return result


class DryingTests(unittest.TestCase):
    def setUp(self):
        package = module('drying_test_integration')
        package.__path__ = [str(Path(__file__).resolve().parents[1] / 'custom_components/dyson_spot_scrub')]
        class Coordinator:
            pass
        features = IntFlag('Features', {name: 1 << i for i, name in enumerate(['START', 'STOP', 'PAUSE', 'RETURN_HOME', 'FAN_SPEED', 'STATE'])})
        modules = {
            'drying_test_integration': package,
            'paho': module('paho'), 'paho.mqtt': module('paho.mqtt'),
            'paho.mqtt.client': module('paho.mqtt.client'),
            'drying_test_integration.coordinator': module('coordinator', DysonCoordinator=Coordinator),
            'voluptuous': module('voluptuous', Schema=lambda x: x, Required=lambda x: x, Optional=lambda x, **kw: x, All=lambda *x: x),
            'homeassistant': module('homeassistant'),
            'homeassistant.components': module('components'),
            'homeassistant.components.vacuum': module('vacuum', StateVacuumEntity=object, VacuumEntityFeature=features, VacuumActivity=SimpleNamespace()),
            'homeassistant.config_entries': module('config_entries', ConfigEntry=object),
            'homeassistant.core': module('core', HomeAssistant=object, ServiceCall=object, callback=lambda f: f),
            'homeassistant.exceptions': module('exceptions', HomeAssistantError=RuntimeError),
            'homeassistant.helpers': module('helpers'),
            'homeassistant.helpers.config_validation': module('cv', string=str, entity_id=str, ensure_list=lambda v: v if isinstance(v, list) else [v]),
            'homeassistant.helpers.entity': module('entity', DeviceInfo=dict),
            'homeassistant.helpers.entity_platform': module('platform', AddEntitiesCallback=object),
            'homeassistant.helpers.entity_registry': module('registry', async_get=lambda hass: self.registry),
        }
        modules['homeassistant.helpers'].config_validation = modules['homeassistant.helpers.config_validation']
        modules['homeassistant.helpers'].entity_registry = modules['homeassistant.helpers.entity_registry']
        self.enterContext(patch.dict(sys.modules, modules))
        # Reload these modules so each test uses this test's stub coordinator class.
        for name in ['drying_test_integration.vacuum', 'drying_test_integration.dyson_mqtt']:
            sys.modules.pop(name, None)
        self.mqtt = importlib.import_module('drying_test_integration.dyson_mqtt')
        self.vacuum = importlib.import_module('drying_test_integration.vacuum')
        self.client = self.mqtt.DysonMqttClient('test', 'RB05', 'invalid', 'token', 'signature', 'client', 'auth')
        self.client.connected = True
        self.client.state = {'dockState': 'DRYING_MOP'}
        self.sent = self.enterContext(patch.object(self.client, '_publish', return_value=True))
        self.coord = Coordinator()
        self.coord.mqtt = self.client
        self.coord.state = self.client.state
        self.registry = SimpleNamespace(async_get=lambda eid: SimpleNamespace(platform='dyson_spot_scrub', config_entry_id='one', entity_id=eid))
        self.services = {}
        async def executor(fn, *args):
            return fn(*args)
        self.hass = SimpleNamespace(data={'dyson_spot_scrub': {'one': self.coord}}, async_add_executor_job=executor,
            services=SimpleNamespace(has_service=lambda d, n: n in self.services, async_register=lambda d,n,handler,**kw: self.services.update({n: handler})))
        self.entry = SimpleNamespace(entry_id='one', data={'serial': 'test', 'device_name': 'Dyson'})
        self.entities = []
        asyncio.run(self.vacuum.async_setup_entry(self.hass, self.entry, self.entities.extend))

    def sample(self, remaining=6660, total=14400, kind=3):
        self.client._handle_jdm({'method': 'prop.post', 'params': {'work_time': {'type': kind, 'total': total, 'surplus': remaining}}})
        self.coord.state = self.client.state

    def test_samples_and_unrelated_status_keep_measurement_timestamp(self):
        with patch.object(self.mqtt, '_now_iso', return_value='2026-10-19T23:20:42Z'):
            self.sample()
        attrs = self.entities[0].extra_state_attributes
        self.assertEqual(attrs['drying_total_seconds'], 14400)
        self.assertEqual(attrs['drying_remaining_seconds'], 6660)
        self.client._handle_status({'msg': 'CURRENT-STATE', 'dockState': 'DRYING_MOP', 'batteryChargeLevel': 100})
        self.assertEqual(self.client.state['drying_updated_at'], '2026-10-19T23:20:42Z')
        with patch.object(self.mqtt, '_now_iso', return_value='2026-10-19T23:23:42Z'):
            self.sample(6480)  # Missing packets need no retry or synthetic decrement.
        self.assertEqual(self.client.state['drying_remaining_seconds'], 6480)
        self.assertEqual(self.client.state['drying_updated_at'], '2026-10-19T23:23:42Z')

    def test_invalid_samples_do_not_replace_valid_measurement(self):
        self.sample()
        initial = dict(self.client.state)
        for remaining, total, kind in [(-1,14400,3),(14401,14400,3),(0,0,3),(None,14400,3),(True,14400,3),(100,14400,2),(100,14400,True),('60',14400,3)]:
            self.sample(remaining,total,kind)
            self.assertEqual(self.client.state, initial)
        self.client._handle_jdm({'method':'prop.post','params':{'work_time':None}})
        self.assertEqual(self.client.state, initial)

    def test_end_and_new_cycle_do_not_reuse_old_times(self):
        self.sample(0)
        self.assertEqual(self.client.state['dockState'], 'DRYING_MOP')
        self.client._handle_status({'msg':'STATE-CHANGE','dockState':'IDLE'})
        self.assertNotIn('drying_total_seconds', self.client.state)
        self.sample()  # Late timing packet after completion must not reactivate it.
        self.assertNotIn('drying_remaining_seconds', self.client.state)
        self.client._handle_status({'msg':'STATE-CHANGE','dockState':'DRYING_MOP'})
        self.coord.state = self.client.state
        self.assertNotIn('drying_remaining_seconds', self.entities[0].extra_state_attributes)
        self.sample(14400)
        self.assertEqual(self.entities[0].extra_state_attributes['drying_remaining_seconds'],14400)

    def test_disconnect_clears_timing(self):
        self.sample()
        self.client._on_disconnect(None,None,1)
        self.coord.state = self.client.state
        self.assertNotIn('drying_remaining_seconds',self.entities[0].extra_state_attributes)
        self.assertNotIn('drying_updated_at',self.client.state)

    def call_cancel(self, entities=None):
        asyncio.run(self.services['cancel_drying'](SimpleNamespace(data={'entity_id': entities or ['vacuum.dyson']})))

    def test_cancel_sends_existing_command_without_fake_completion(self):
        self.sample()
        before = dict(self.client.state)
        self.call_cancel()
        payload = self.sent.call_args.args[0]
        self.assertEqual(payload['msg'], 'ABORT-DOCK-ACTION')
        self.assertEqual(payload['action'], 'DRY_MOP')
        self.assertEqual(payload['mode-reason'], 'RAPP')
        self.sent.assert_called_once()
        self.assertEqual(self.client.state, before)

    def test_action_rejects_invalid_target_inactive_and_disconnected(self):
        for entities in [['vacuum.a','vacuum.b'],['switch.not_a_vacuum']]:
            with self.assertRaises(RuntimeError): self.call_cancel(entities)
        self.coord.state = {'dockState':'IDLE'}
        with self.assertRaises(RuntimeError): self.call_cancel()
        self.coord.state = {'dockState':'DRYING_MOP'}
        self.client.connected = False
        with self.assertRaises(RuntimeError): self.call_cancel()
        self.sent.assert_not_called()

    def test_publish_failure_is_reported(self):
        self.sent.return_value = False
        with self.assertRaises(RuntimeError): self.call_cancel()
        self.assertEqual(self.client.state['dockState'],'DRYING_MOP')


if __name__ == '__main__':
    unittest.main()
