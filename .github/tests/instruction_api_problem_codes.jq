# Slurp so a bundler failure producing no JSON also fails the check.
length == 1 and (.[0] |
  (.paths["/v1/instructions"].post.responses["413"].description |
    startswith("payload_too_large.")) and
  (.components.schemas.Problem.properties.type.enum |
    index("https://jammy.dev/problems/payload_too_large") != null and
    index("https://jammy.dev/problems/body_too_large") == null)
)
