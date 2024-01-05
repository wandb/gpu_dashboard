QUERY = """\
query GetGpuInfoForProject($project: String!, $entity: String!) {
  project(name: $project, entityName: $entity) {
    name
    runs {
      edges {
        node {
          name
          user {
            username
          }
          computeSeconds
          createdAt
          updatedAt
          runInfo {
            gpuCount
            gpu
          }
        }
      }
    }
  }
}\
"""
