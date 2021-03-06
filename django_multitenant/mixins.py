import logging

from django.db import models
from django.db.models.sql import DeleteQuery
from django.db.models.deletion import Collector

from .deletion import related_objects
from .query import wrap_get_compiler
from .utils import (
    set_current_tenant,
    get_current_tenant,
    get_current_tenant_value,
    get_model_by_db_table,
    get_tenant_column,
    get_tenant_filters,
)


logger = logging.getLogger(__name__)


class TenantManagerMixin(object):
    # Below is the manager related to the above class.
    def get_queryset(self):
        # Injecting tenant_id filters in the get_queryset.
        # Injects tenant_id filter on the current model for all the non-join/join queries.
        queryset = self._queryset_class(self.model)
        current_tenant = get_current_tenant()
        if current_tenant:
            kwargs = get_tenant_filters(self.model)
            return queryset.filter(**kwargs)
        return queryset


class TenantModelMixin(object):
    # Abstract model which all the models related to tenant inherit.
    tenant_id = ""

    def __init__(self, *args, **kwargs):
        if not hasattr(DeleteQuery.get_compiler, "_sign"):
            DeleteQuery.get_compiler = wrap_get_compiler(DeleteQuery.get_compiler)
            Collector.related_objects = related_objects

        super(TenantModelMixin, self).__init__(*args, **kwargs)

    def _do_update(self, base_qs, using, pk_val, values, update_fields, forced_update):
        # adding tenant filters for save
        # Citus requires tenant_id filters for update, hence doing this below change.

        model = base_qs.model
        current_tenant = get_current_tenant()

        if current_tenant:
            kwargs = get_tenant_filters(self.__class__)
            base_qs = base_qs.filter(**kwargs)
        else:
            logger.warning(
                'Attempting to update %s instance "%s" without a current tenant '
                "set. This may cause issues in a partitioned environment. "
                "Recommend calling set_current_tenant() before performing this "
                "operation.",
                self._meta.model.__name__,
                self,
            )

        return super(TenantModelMixin, self)._do_update(
            base_qs, using, pk_val, values, update_fields, forced_update
        )

    def save(self, *args, **kwargs):
        tenant_value = get_current_tenant_value()
        if not self.pk and tenant_value and not isinstance(tenant_value, list):
            setattr(self, self.tenant_field, tenant_value)

        return super(TenantModelMixin, self).save(*args, **kwargs)

    @property
    def tenant_field(self):
        return self.tenant_id

    @property
    def tenant_value(self):
        return getattr(self, self.tenant_field, None)
