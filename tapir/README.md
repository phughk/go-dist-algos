# TAPIR
TAPIR is a Transactional Application Protocol for Inconsistent Replication (IR).

## Inconsistent Replication
IR uses four sub-protocols
 - operation processing
 - replica recovery/synchronization
 - client recovery
 - group membership change

Due to space constraints, the [Building Consistent Transactions with Inconsistent Replication](tapir.pdf) only covers the first two here; the third is described in [Building Consistent Transactions with Inconsistent Replication (Extended Version)](tapir-tr-v2.pdf) and the the last is identical to that of [Viewstamped Replication](vr-revisited.pdf).

