# syntax=docker/dockerfile:1.7
#
# One image definition for the four Spring Boot services, selected with --build-arg MODULE=<module>.
# Build context: the repository root (the root pom.xml is the parent of every module).
#   docker build -f spring-webflux-poc/Dockerfile --build-arg MODULE=ride-service .

# ---- build: Maven on Temurin 21 ----
FROM maven:3.9.16-eclipse-temurin-21-noble AS build
ARG MODULE
WORKDIR /workspace
COPY pom.xml ./
COPY spring-webflux-poc ./spring-webflux-poc
# The cache mount keeps ~/.m2 between builds and shares it between the four images, so a rebuild
# only downloads what changed. sharing=locked serializes the parallel builds on the shared cache.
# Tests run in the CI Maven job, not in the image build.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    test -n "$MODULE" || { echo "MODULE build arg is required" >&2; exit 1; }; \
    mvn -B -q -pl "spring-webflux-poc/${MODULE}" -am package -DskipTests \
    && cp "spring-webflux-poc/${MODULE}/target/${MODULE}-"*.jar /workspace/app.jar

# ---- runtime: JRE only, non-root ----
FROM eclipse-temurin:21.0.13.1_2-jre-noble
RUN groupadd --system app && useradd --system --gid app --no-create-home app
WORKDIR /app
COPY --from=build --chown=app:app /workspace/app.jar /app/app.jar
USER app
# Size the heap from the container memory limit instead of the host's RAM.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/app.jar"]
