"""Proportional allocation in integer milli-kilowatts."""

from decimal import Decimal


def allocate(
    target_kw: Decimal,
    assets: list[tuple[str, Decimal]],
) -> tuple[dict[str, Decimal], str]:
    capacity = {asset: int(max(power, 0) * 1000) for asset, power in assets}
    target = int(target_kw * 1000)
    total = sum(capacity.values())
    if target >= total:
        status = "dispatched" if total and target == total else "partially_dispatched"
        return {
            asset: Decimal(value).scaleb(-3) for asset, value in capacity.items()
        }, status

    shares = {asset: divmod(target * value, total) for asset, value in capacity.items()}
    remaining = target - sum(base for base, _ in shares.values())
    ranked = sorted(
        capacity, key=lambda asset: (-shares[asset][1], -capacity[asset], asset)
    )
    extra = set(ranked[:remaining])
    return {
        asset: Decimal(base + (asset in extra)).scaleb(-3)
        for asset, (base, _) in shares.items()
    }, "dispatched"
