#!/usr/bin/env bash
set -euo pipefail

root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$root"
chart=deploy/helm/jammy-platform
compose_dir=deploy/compose
registry=ghcr.io/cloud4-final/jammy-application
impl=${IMPL:-go}
tag=${TAG:?Set TAG to the version-sha7 or release tag of the local images}
case "$impl" in go|py) ;; *) echo "IMPL must be go or py" >&2; exit 2 ;; esac

[[ -f "$compose_dir/docker-compose.yml" ]] || { echo "T3 Compose stack is missing" >&2; exit 1; }
images=()
for service in instruction-api dispatcher "ingest-$impl" "processor-$impl" "archiver-$impl" portal-publisher plantsim migrate; do
    image="$registry/$service:$tag"
    docker image inspect "$image" >/dev/null
    images+=("$image")
done

run_arm() (
    arm=$1
    project="jammy-helm-$arm-$$"
    network="${project}_default"
    export COMPOSE_PROJECT_NAME="$project"
    compose=(docker compose -f "$compose_dir/docker-compose.yml" --profile "$arm")
    kubeconfig=$(mktemp)
    new_cluster=false
    k() {
        docker exec -i "k3d-$project-server-0" k3s kubectl "$@"
    }
    # Invoked by the EXIT trap below.
    # shellcheck disable=SC2329
    cleanup() {
        result=$?
        set +e
        if "$new_cluster"; then
            if (( result != 0 )); then
                k -n jammy get pods,events -o wide >&2
            fi
            k3d cluster delete "$project"
        fi
        "${compose[@]}" down --volumes
        rm -f "$kubeconfig"
        exit "$result"
    }
    trap cleanup EXIT

    dependencies=(pg-primary seaweedfs elasticmq dynamodb-local)
    agents=1
    if [[ $arm == split ]]; then
        dependencies+=(pg-standby nats)
        agents=4
        export JAMMY_SYNC_STANDBY='ANY 1 (standby1)'
    else
        export JAMMY_SYNC_STANDBY=''
    fi
    "${compose[@]}" up --wait --wait-timeout 120 "${dependencies[@]}"
    "$compose_dir/aws/init-aws.sh" init

    container_ip() {
        local id
        id=$("${compose[@]}" ps --quiet "$1")
        docker inspect --format "{{with index .NetworkSettings.Networks \"$network\"}}{{.IPAddress}}{{end}}" "$id"
    }

    new_cluster=true
    k3d cluster create "$project" --agents "$agents" --network "$network" \
        --image rancher/k3s:v1.37.2-k3s1 --no-lb \
        --kubeconfig-update-default=false --kubeconfig-switch-context=false \
        --k3s-arg '--disable=traefik,servicelb@server:*' --timeout 180s
    k3d kubeconfig get "$project" > "$kubeconfig"
    k3d image import --cluster "$project" "${images[@]}"
    if [[ $arm == mvp ]]; then
        k label node "k3d-$project-agent-0" role=main
        lease=(--set-string dispatcher.env.LEASE_MODE=disabled)
    else
        for i in 0 1; do
            k label node "k3d-$project-agent-$i" role=control
            k taint node "k3d-$project-agent-$i" role=control:NoSchedule
        done
        k label node "k3d-$project-agent-2" role=ingest jammy.io/ingest-instance=a1
        k label node "k3d-$project-agent-3" role=ingest jammy.io/ingest-instance=a2
        lease=(
            --set-string dispatcher.env.LEASE_MODE=dynamodb
            --set-string dispatcher.env.LEASE_TABLE=jammy-site-lease
            --set-string dispatcher.env.LEASE_AUTO_ACQUIRE=true
            --set-string "dispatcher.env.DYNAMODB_ENDPOINT=http://$(container_ip dynamodb-local):8000"
            --set-string "global.nats.url=nats://$(container_ip nats):4222"
        )
    fi

    k create namespace jammy
    k -n jammy create secret generic jammy-smoke \
        --from-literal="DATABASE_URL=postgres://jammy:jammy@$(container_ip pg-primary):5432/jammy?sslmode=disable" \
        --from-literal='HMAC_KEYS={"key-a":"fixture-secret"}' \
        --from-literal=AWS_ACCESS_KEY_ID=jammy \
        --from-literal=AWS_SECRET_ACCESS_KEY=jammy
    helm install jammy-platform "$chart" --kubeconfig "$kubeconfig" --namespace jammy \
        -f "$chart/values-$arm.yaml" -f deploy/helm/smoke-values.yaml --wait=watcher --timeout 5m \
        --set-string "global.image.tag=$tag" \
        --set-string global.existingSecret=jammy-smoke \
        --set-string global.s3.bucket=jammy-local \
        --set-string "global.s3.endpoint=http://$(container_ip seaweedfs):8333" \
        --set global.s3.forcePathStyle=true \
        --set-string "ingest.impl=$impl,processor.impl=$impl,archiver.impl=$impl" \
        --set plantsim.enabled=true \
        --set-string dispatcher.env.PLANT_GATEWAY_URL=http://jammy-platform-plantsim:8080 \
        --set-string dispatcher.env.AWS_REGION=ap-northeast-2 \
        --set-string dispatcher.env.AWS_DEFAULT_REGION=ap-northeast-2 \
        "${lease[@]}"
    k -n jammy wait --for=condition=Ready pods --all --timeout=120s
    k -n jammy get pods -o wide
)

run_arm mvp
run_arm split
