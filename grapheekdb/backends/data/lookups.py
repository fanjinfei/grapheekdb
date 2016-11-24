# -*- coding:utf-8 -*-


def lookup_exact(reference_value, field_value):
    return field_value == reference_value


def lookup_iexact(reference_value, field_value):
    return lookup_exact(field_value.lower(), reference_value.lower())


def lookup_contains(reference_value, field_value):
    return reference_value in field_value


def lookup_icontains(reference_value, field_value):
    return reference_value.lower() in field_value.lower()


def lookup_in(reference_value, field_value):
    return field_value in reference_value


def lookup_gt(reference_value, field_value):
    return field_value > reference_value


def lookup_gte(reference_value, field_value):
    return field_value >= reference_value


def lookup_lt(reference_value, field_value):
    return field_value < reference_value


def lookup_lte(reference_value, field_value):
    return field_value <= reference_value


def lookup_startswith(reference_value, field_value):
    return field_value.startswith(reference_value)


def lookup_istartswith(reference_value, field_value):
    return lookup_startswith(reference_value.lower(), field_value.lower())


def lookup_endswith(reference_value, field_value):
    return field_value.endswith(reference_value)


def lookup_iendswith(reference_value, field_value):
    return lookup_endswith(reference_value.lower(), field_value.lower())


def lookup_isnull(reference_value, field_value):
    return (field_value is None) == reference_value
