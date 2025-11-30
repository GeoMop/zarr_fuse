import numpy as np
import pytest

from zarr_fuse import units

def test_quanity():
    q = units.Quantity(np.array([1, 2, 3]), 'meter')
    assert isinstance(q, units.Quantity)
    assert np.allclose(q.to('centimeter').magnitude, [100, 200, 300])

    # Interoperability with pint Quantity, common registry sould be used due to use of set_application_registry
    raw_q = units._pint.Quantity(np.array([1, 2, 3]), 'meter')
    assert np.all(raw_q == q)



# TODO: replace by Variable.convert_value
@pytest.mark.skip
def test_create_quantity():
    from datetime import timezone, timedelta
    # Numeric conversion correctness
    q_numeric = units.create_quantity(['1', '2.5', '3'], 'meter')
    assert isinstance(q_numeric, units.Quantity)
    assert np.allclose(q_numeric.to('millimeter').magnitude, [1000, 2500, 3000])


    # Datetime parsing with mixed syntaxes and single UTC config
    tick = 'h'
    expected = np.array(['2021-12-31T23'], dtype=f'datetime64[{tick}]')

    def check_dt(cfg, dates, expected_val):
        qd = units.create_quantity(dates, cfg)
        expected_val = np.full_like(dates, expected_val[0], dtype=expected_val.dtype)
        assert np.array_equal(qd.magnitude, expected_val)

        # Parser tests with inline check_dt calls:

    check_dt(
        cfg=units.DateTimeUnit(tick, tz='+00:00', dayfirst=False, yearfirst=False),
        dates=['2021-12-31 23:00', '31 Dec 2021 23:00'],
        expected_val=expected
    )
    check_dt(
        cfg=units.DateTimeUnit(tick, tz='+00:00', dayfirst=False, yearfirst=False),
        dates=['2021-12-31T23:00:00', '31 Dec 2021 23:00'],
        expected_val=expected
    )
    check_dt(
        cfg=units.DateTimeUnit(tick, tz='+00:00', dayfirst=True, yearfirst=False),
        dates=['31/12/2021 23:00', 'Dec 31 2021 23:00'],
        expected_val=expected
    )
    check_dt(
        cfg=units.DateTimeUnit(tick, tz='+00:00', dayfirst=False, yearfirst=True),
        dates=['21-12-31 23:00', '12/31/21 23:00'],
        expected_val=expected
    )
    # Timezone variations mapping to same UTC
    check_dt(
        cfg=units.DateTimeUnit(tick, tz='+00:00', dayfirst=False, yearfirst=True),
        dates=['22/01/01 00:00 CET', '21/12/31 23:00', '21/12/31T23:00Z', '22/01/01 02:00+03:00'],
        expected_val=expected
    )
    # Vary tick and tz
    check_dt(
        cfg=units.DateTimeUnit('m', tz='+02:00', dayfirst=False, yearfirst=False),
        dates=['2021-12-31 19:00 UTC', '31 Dec 2021 19:00 UTC'],
        expected_val=np.array(['2021-12-31T21:00', '2021-12-31T21:00'], dtype='datetime64[m]')
    )


    # Test .to()
    target = units.DateTimeUnit(tick='m', tz='+02:00', dayfirst=False, yearfirst=False)
    dates_base = ['2021-12-31 21:00', '31 Dec 2021 21:00']
    qd_m = units.create_quantity(
        dates_base,
        units.DateTimeUnit(tick, tz='+00:00', dayfirst=False, yearfirst=False)
    ).to(target)
    expected_m = np.array(['2021-12-31T23:00', '2021-12-31T23:00'], dtype='datetime64[m]')
    assert np.array_equal(qd_m.magnitude, expected_m)

    # Test __add__
    addition = units.Quantity(90, 'minute')
    qd_added = qd_m + addition
    expected_added = np.array(['2022-01-01T00:30', '2022-01-01T00:30'], dtype='datetime64[m]')
    assert np.array_equal(qd_added.magnitude, expected_added)
