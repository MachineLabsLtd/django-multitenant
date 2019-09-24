import logging
from django.db import models

from .utils import (
    get_current_tenant,
    get_current_tenant_value,
    get_tenant_column,
    get_tenant_field,
    get_tenant_filters,
)

logger = logging.getLogger(__name__)


class TenantIDFieldMixin:
    """
    Since migrations shouldn't import the models they depend on,
    and we need Model.tenant_id to figure out how to create our composite constraints for sharding,
    store tenant_id on the field itself and add it to the migrations.
    """

    def __init__(self, *args, **kwargs):
        self.tenant_id = kwargs.pop("tenant_id", None)
        super().__init__(*args, **kwargs)

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        kwargs["tenant_id"] = self.tenant_id
        return name, path, args, kwargs


class TenantForeignKey(models.ForeignKey):
    """
    Should be used in place of models.ForeignKey for all foreign key relationships to
    subclasses of TenantModel.

    Adds additional clause to JOINs over this relation to include tenant_id in the JOIN
    on the TenantModel.

    Adds clause to forward accesses through this field to include tenant_id in the
    TenantModel lookup.
    """

    # Get override
    def get_joining_columns(self, reverse_join=False):
        default_columns = super().get_joining_columns(reverse_join=reverse_join)

        # Add join on tenant IDs
        lhs_tenant_id = get_tenant_column(self.model)
        rhs_tenant_id = get_tenant_column(self.related_model)
        return default_columns + ((lhs_tenant_id, rhs_tenant_id),)

    # Override
    def get_extra_descriptor_filter(self, instance):
        """
        Return an extra filter condition for related object fetching when
        user does 'instance.fieldname', that is the extra filter is used in
        the descriptor of the field.

        The filter should be either a dict usable in .filter(**kwargs) call or
        a Q-object. The condition will be ANDed together with the relation's
        joining columns.

        A parallel method is get_extra_restriction() which is used in
        JOIN and subquery conditions.
        """
        current_tenant = get_current_tenant()
        if not current_tenant:
            raise ValueError(
                "TenantForeignKey field %s.%s accessed without a current tenant set.",
                self.model.__name__,
                self.name,
            )
        return get_tenant_filters(instance)

    # Override
    def get_extra_restriction(self, where_class, alias, related_alias):
        """
        Return a pair condition used for joining and subquery pushdown. The
        condition is something that responds to as_sql(compiler, connection)
        method.

        Note that currently referring both the 'alias' and 'related_alias'
        will not work in some conditions, like subquery pushdown.

        A parallel method is get_extra_descriptor_filter() which is used in
        instance.fieldname related object fetching.
        """

        current_tenant_value = get_current_tenant_value()
        if not current_tenant_value:
            return None

        # Fetch tenant column names for both sides of the relation
        lhs_tenant_field = get_tenant_field(self.model)
        lookup_lhs = lhs_tenant_field.get_col(related_alias)

        # Create "AND lhs.tenant_id = current_tenant_value as a new condition
        lookup = lhs_tenant_field.get_lookup("exact")(lookup_lhs, current_tenant_value)
        condition = where_class()
        condition.add(lookup, "AND")
        return condition

    def _check_unique_target(self):
        # Disable "<field> must set unique=True because it is referenced by a foreign key." error (ID fields.E311),
        # as we can't enforce that with composite keys.
        return []


class TenantOneToOneField(models.OneToOneField, TenantIDFieldMixin, TenantForeignKey):
    # Override
    def __init__(self, *args, **kwargs):
        kwargs["unique"] = False
        super(TenantOneToOneField, self).__init__(*args, **kwargs)


class TenantPrimaryKey(TenantIDFieldMixin, models.AutoField):
    def __init__(self, *args, **kwargs):
        kwargs["primary_key"] = True
        super().__init__(*args, **kwargs)

    def _check_primary_key(self):
        # Disable "AutoFields must set primary_key=True" error (ID fields.E100),
        # as we can't enforce that with composite keys.
        return []
