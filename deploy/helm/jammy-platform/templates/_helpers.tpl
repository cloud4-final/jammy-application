{{- define "jammy.namespace" -}}
{{- if eq .Release.Namespace "default" -}}jammy{{- else -}}{{ .Release.Namespace }}{{- end -}}
{{- end -}}

{{- define "jammy.name" -}}
{{- $prefixLength := int (sub 62 (len .component)) -}}
{{- printf "%s-%s" (.root.Release.Name | trunc $prefixLength | trimSuffix "-") .component -}}
{{- end -}}

{{- define "jammy.selector" -}}
app.kubernetes.io/name: jammy-platform
app.kubernetes.io/instance: {{ .root.Release.Name | quote }}
app.kubernetes.io/component: {{ .component | quote }}
{{- end -}}

{{- define "jammy.image" -}}
{{- $tag := required "global.image.tag is required" .root.Values.global.image.tag -}}
{{- if not (regexMatch "^[0-9]+[.][0-9]+[.][0-9]+(-[a-f0-9]{7})?$" $tag) -}}
{{- fail "global.image.tag must be a version or version-sha7" -}}
{{- end -}}
{{- printf "%s/%s:%s" (.root.Values.global.image.registry | trimSuffix "/") .image $tag -}}
{{- end -}}

{{- define "jammy.security" -}}
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}

{{- define "jammy.podSettings" -}}
automountServiceAccountToken: false
securityContext:
  runAsNonRoot: true
  seccompProfile:
    type: RuntimeDefault
  {{- if .stageDir }}
  fsGroup: 65532
  {{- end }}
{{- with .root.Values.global.imagePullSecrets }}
imagePullSecrets:
  {{- range . }}
  - name: {{ . | quote }}
  {{- end }}
{{- end }}
{{- with .nodeSelector }}
nodeSelector:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- with .app.tolerations }}
tolerations:
  {{- toYaml . | nindent 2 }}
{{- end }}
{{- end -}}

{{- define "jammy.secretEnv" -}}
- name: {{ .key }}
  valueFrom:
    secretKeyRef:
      name: {{ required "global.existingSecret is required for the enabled services" .global.existingSecret | quote }}
      key: {{ .key }}
      {{- if .optional }}
      optional: true
      {{- end }}
{{- end -}}

{{- define "jammy.workload" -}}
{{- $root := .root -}}
{{- $global := $root.Values.global -}}
{{- $svc := .service -}}
{{- $app := .app -}}
{{- $pipeline := has $svc (list "ingest" "processor" "archiver") -}}
{{- $jetstream := and $pipeline (eq $global.pipelineMode "jetstream") -}}
{{- $database := or (has $svc (list "instruction-api" "dispatcher" "processor" "portal-publisher")) (and $pipeline (not $jetstream)) -}}
{{- $s3 := has $svc (list "archiver" "portal-publisher") -}}
{{- $hmac := eq $svc "instruction-api" -}}
{{- $natsCreds := and $jetstream $global.nats.existingSecret -}}
{{- $http := has $svc (list "instruction-api" "ingest" "plantsim") -}}
{{- $dispatcherHA := and (eq $svc "dispatcher") (gt (int $app.replicas) 1) -}}
{{- $image := $svc -}}
{{- if $pipeline }}
  {{- $image = printf "%s-%s" $svc $app.impl }}
{{- end }}
{{- $env := deepCopy $app.env -}}
{{- $_ := set $env "ADMIN_PORT" "9090" -}}
{{- $_ := set $env "OTEL_RESOURCE_ATTRIBUTES" (printf "service.version=%s,deployment.environment=%s" $global.image.tag $global.environment) -}}
{{- if $http }}
  {{- $_ := set $env "HTTP_PORT" "8080" }}
{{- end }}
{{- if $pipeline }}
  {{- $_ := set $env "PIPELINE_MODE" $global.pipelineMode }}
{{- end }}
{{- if $jetstream }}
  {{- $_ := set $env "NATS_URL" (required "global.nats.url is required in jetstream mode" $global.nats.url) }}
  {{- if $natsCreds }}
    {{- $_ := set $env "NATS_CREDS_FILE" "/var/run/jammy/nats/credentials" }}
  {{- end }}
  {{- if eq $svc "ingest" }}
    {{- $_ := set $env "INGEST_STREAM" (required "each ingest instance needs a stream in jetstream mode" .stream) }}
  {{- else }}
    {{- $streams := list }}
    {{- range $root.Values.ingest.instances }}
      {{- $streams = append $streams (required "each ingest instance needs a stream in jetstream mode" .stream) }}
    {{- end }}
    {{- $_ := set $env "NATS_STREAMS" (join "," $streams) }}
  {{- end }}
{{- end }}
{{- if $s3 }}
  {{- $_ := set $env "S3_BUCKET" (required "global.s3.bucket is required for S3 services" $global.s3.bucket) }}
  {{- $_ := set $env "S3_ENDPOINT" (required "global.s3.endpoint is required for S3 services" $global.s3.endpoint) }}
  {{- $_ := set $env "S3_REGION" $global.s3.region }}
  {{- $_ := set $env "S3_FORCE_PATH_STYLE" $global.s3.forcePathStyle }}
{{- end }}
{{- if eq $svc "dispatcher" }}
  {{- $mode := required "dispatcher.env.LEASE_MODE is required" $app.env.LEASE_MODE }}
  {{- if not (has $mode (list "disabled" "dynamodb")) }}
    {{- fail "dispatcher.env.LEASE_MODE must be disabled or dynamodb" }}
  {{- end }}
  {{- $_ := required "dispatcher.env.PLANT_GATEWAY_URL is required" $app.env.PLANT_GATEWAY_URL }}
  {{- if eq $mode "dynamodb" }}
    {{- $_ := required "dispatcher.env.LEASE_TABLE is required in dynamodb mode" $app.env.LEASE_TABLE }}
    {{- if not (has (toString $app.env.LEASE_AUTO_ACQUIRE) (list "true" "false")) }}
      {{- fail "dispatcher.env.LEASE_AUTO_ACQUIRE must be true or false in dynamodb mode" }}
    {{- end }}
  {{- end }}
{{- end }}
{{- if $hmac }}
  {{- $_ := set $env "HMAC_KEYS_FILE" "/var/run/jammy/hmac/keys.json" }}
{{- end }}
{{- $stageDir := "" -}}
{{- if eq $svc "archiver" }}
  {{- $stageDir = default "/stage" $app.env.STAGE_DIR }}
  {{- $_ := set $env "STAGE_DIR" $stageDir }}
{{- end }}
{{- if $database }}
  {{- $_ := unset $env "DATABASE_URL" }}
{{- end }}
{{- $lease := and (eq $svc "dispatcher") (eq (toString $app.env.LEASE_MODE) "dynamodb") -}}
{{- if or $s3 $lease (and (eq $svc "dispatcher") $app.env.SQS_QUEUE_URL) }}
  {{- $_ := unset $env "AWS_ACCESS_KEY_ID" }}
  {{- $_ := unset $env "AWS_SECRET_ACCESS_KEY" }}
{{- end }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "jammy.name" . }}
  namespace: {{ include "jammy.namespace" $root }}
  labels:
    {{- include "jammy.selector" . | nindent 4 }}
spec:
  replicas: {{ $app.replicas }}
  selector:
    matchLabels:
      {{- include "jammy.selector" . | nindent 6 }}
  {{- if eq $svc "ingest" }}
  strategy:
    type: Recreate
  {{- else if $dispatcherHA }}
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 1
  {{- end }}
  template:
    metadata:
      labels:
        {{- include "jammy.selector" . | nindent 8 }}
        jammy.io/service: {{ $image | quote }}
      annotations:
        jammy.io/site: {{ required "global.siteId is required" $global.siteId | quote }}
        jammy.io/scrape: "true"
        jammy.io/port: "9090"
        checksum/config: {{ include "jammy.configData" $root | sha256sum }}
    spec:
      {{- include "jammy.podSettings" (merge (dict "stageDir" $stageDir) .) | nindent 6 }}
      terminationGracePeriodSeconds: 30
      {{- if $dispatcherHA }}
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  {{- include "jammy.selector" . | nindent 18 }}
      {{- end }}
      containers:
        - name: {{ $svc }}
          image: {{ include "jammy.image" (dict "root" $root "image" $image) | quote }}
          securityContext:
            {{- include "jammy.security" . | nindent 12 }}
          resources:
            {{- toYaml $app.resources | nindent 12 }}
          ports:
            {{- if $http }}
            - {name: http, containerPort: 8080}
            {{- end }}
            - {name: admin, containerPort: 9090}
          readinessProbe:
            httpGet: {path: /readyz, port: 9090}
          livenessProbe:
            httpGet: {path: /livez, port: 9090}
          envFrom:
            - configMapRef:
                name: {{ include "jammy.name" (dict "root" $root "component" "config") }}
          env:
            {{- range $key, $value := $env }}
            - name: {{ $key }}
              value: {{ $value | toString | quote }}
            {{- end }}
            {{- if $database }}
            {{- include "jammy.secretEnv" (dict "key" "DATABASE_URL" "global" $global) | nindent 12 }}
            {{- end }}
            {{- if or $s3 $lease (and (eq $svc "dispatcher") $app.env.SQS_QUEUE_URL) }}
            {{- range list "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" }}
            {{- include "jammy.secretEnv" (dict "key" . "global" $global "optional" true) | nindent 12 }}
            {{- end }}
            {{- end }}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
            {{- if $stageDir }}
            - {name: stage, mountPath: {{ $stageDir | quote }}}
            {{- end }}
            {{- if $hmac }}
            - {name: hmac, mountPath: /var/run/jammy/hmac, readOnly: true}
            {{- end }}
            {{- if $natsCreds }}
            - {name: nats, mountPath: /var/run/jammy/nats, readOnly: true}
            {{- end }}
      volumes:
        - name: tmp
          emptyDir: {}
        {{- if $stageDir }}
        - name: stage
          emptyDir: {}
        {{- end }}
        {{- if $hmac }}
        - name: hmac
          secret:
            secretName: {{ required "global.existingSecret is required for instruction-api" $global.existingSecret | quote }}
            items:
              - {key: HMAC_KEYS, path: keys.json}
        {{- end }}
        {{- if $natsCreds }}
        - name: nats
          secret:
            secretName: {{ $global.nats.existingSecret | quote }}
            items:
              - {key: NATS_CREDS, path: credentials}
        {{- end }}
{{- end -}}

{{- define "jammy.service" -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "jammy.name" . }}
  namespace: {{ include "jammy.namespace" .root }}
  labels:
    {{- include "jammy.selector" . | nindent 4 }}
spec:
  type: {{ .type }}
  selector:
    {{- include "jammy.selector" . | nindent 4 }}
  ports:
    {{- if .http }}
    - name: http
      port: 8080
      targetPort: http
      {{- with .nodePort }}
      nodePort: {{ . }}
      {{- end }}
    {{- end }}
    {{- if eq .type "ClusterIP" }}
    - name: admin
      port: 9090
      targetPort: admin
    {{- end }}
{{- end -}}

{{- define "jammy.pdb" -}}
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: {{ include "jammy.name" . }}
  namespace: {{ include "jammy.namespace" .root }}
  labels:
    {{- include "jammy.selector" . | nindent 4 }}
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      {{- include "jammy.selector" . | nindent 6 }}
{{- end -}}

{{- define "jammy.configData" -}}
SITE_ID: {{ required "global.siteId is required" .Values.global.siteId | quote }}
LOG_LEVEL: "info"
SHUTDOWN_TIMEOUT_SECONDS: "25"
OTEL_EXPORTER_OTLP_ENDPOINT: {{ .Values.global.otlpEndpoint | quote }}
{{- end -}}
