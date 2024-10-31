import static java.util.stream.Collectors.*;

record OwnedVehicle(String personId, String vehicleId) {}

record Policy(String personId, String vehicleId) {}

record UpsellOpportunity(String personId, String vehicleId) {}

List<OwnedVehicle> getOwnedVehicles(String... personIds) {
  return List.of(
    new OwnedVehicle("P1", "V8"),
    new OwnedVehicle("P1", "V3"),
    new OwnedVehicle("P2", "V6")
  );
}

record Zip(int index, String personId) {}

List<UpsellOpportunity> findPotentialUpsells(Policy... policies) {
  return personIdSet(policies)
    .stream()
    .flatMap(this::zipAndChunk)
    .flatMap(it -> getOwnedVehicles(it).stream())
    .filter(it -> Stream.of(policies).noneMatch(policy -> policy.personId.equals(it.personId) && it.vehicleId.equals(policy.vehicleId)))
    .distinct()
    .map(it -> new UpsellOpportunity(it.personId, it.vehicleId))
    .toList();
}

Stream<String[]> zipAndChunk(String[] personIds) {
  return IntStream.range(0, personIds.length)
    .mapToObj(it -> new Zip(it, personIds[it]))
    .collect(groupingBy(it -> it.index / 100))
    .values()
    .stream()
    .map(Collection::stream)
    .map(it -> it.map(Zip::personId))
    .map(it -> it.toArray(String[]::new));
}

Optional<String[]> personIdSet(Policy[] policies) {
  return Optional.of(Stream.of(policies).map(it -> it.personId).distinct().toArray(String[]::new));
}

void main() {
  shouldFindUpsells();
}

void shouldFindUpsells() {
  // for testing purposes, we will create 200 policies
  var manyPolicies = IntStream.range(0, 202)
    .mapToObj(it -> new Policy("P%d".formatted(it), "V%d".formatted(it)))
    .toArray(size -> new Policy[size]);

  var policies = new Policy[]{
    new Policy("P1", "V8"),
    new Policy("P2", "V6")
  };

  manyPolicies[0] = policies[0];
  manyPolicies[1] = policies[1];

  var actual = findPotentialUpsells(manyPolicies);
  var expected = List.of(new UpsellOpportunity("P1", "V3"));

  assert actual.equals(expected) : "Can't find upsell opportunities, actual: %s, expected: %s".formatted(actual, expected);
  System.out.printf("Upsell opportunities found: %s%n", actual);
}
