#!/usr/bin/env python

__author__ = 'Michael Meisinger'

from pyon.public import CFG, IonObject, log
from pyon.ion.public import RT, AT

from interface.services.coi.iorg_management_service import BaseOrgManagementService

class OrgManagementService(BaseOrgManagementService):

    def create_org(self, name=''):
        log.debug("create_org" + name)
        org = IonObject(RT.Org, dict(name=name))
        rid,rev = self.clients.resource_registry.create(org)
        return rid

    def affiliate_org(self, org_id='', affiliate_org_id=''):
        # Return Value
        # ------------
        # null
        # ...
        # 
        pass

    def enroll_member(self, org_id='', user_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def share_resource(self, org_id='', resource_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def leave(self, org_id='', user_id=''):
        # Return Value
        # ------------
        # null
        # ...
        #
        pass

    def has_permission(self, org_id='', user_id='', action_id='', resource_id=''):
        # Return Value
        # ------------
        # {has_permission: false}
        # 
        pass

