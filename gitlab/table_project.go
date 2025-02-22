package gitlab

import (
	"context"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
	api "github.com/xanzy/go-gitlab"
)

// TODO: Figure out being able to use full_path as a key for the get function, currently seems to fail in gitlab api wrapper.

func projectColumns() []*plugin.Column {
	return []*plugin.Column {
		{Name: "id", Type: proto.ColumnType_INT, Description: "The ID of the project."},
		{Name: "name", Type: proto.ColumnType_STRING, Description: "The projects name."},
		{Name: "path", Type: proto.ColumnType_STRING, Description: "The projects path."},
		{Name: "description", Type: proto.ColumnType_STRING, Description: "The projects description."},
		{Name: "default_branch", Type: proto.ColumnType_STRING, Description: "The projects default branch name."},
		{Name: "full_name", Type: proto.ColumnType_STRING, Description: "The projects name including namespace.", Transform: transform.FromField("NameWithNamespace")},
		{Name: "full_path", Type: proto.ColumnType_STRING, Description: "The projects path including namespace.", Transform: transform.FromField("PathWithNamespace")},
		{Name: "public", Type: proto.ColumnType_BOOL, Description: "Indicates if the project is public"},
		{Name: "visibility", Type: proto.ColumnType_STRING, Description: "The projects visibility level (private/public/internal)"},
		{Name: "web_url", Type: proto.ColumnType_STRING, Description: "The projects url."},
		{Name: "tag_list", Type: proto.ColumnType_JSON, Description: "An array of tags associated to the project."},
		{Name: "issues_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if project has issues enabled."},
		{Name: "open_issues_count", Type: proto.ColumnType_INT, Description: "A count of open issues on the project.", Transform: transform.FromGo()},
		{Name: "merge_requests_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if merge requests are enabled on the project"},
		{Name: "approvals_before_merge", Type: proto.ColumnType_INT, Description: "The project setting for number of approvals required before a merge request can be merged.", Transform: transform.FromGo()},
		{Name: "jobs_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has jobs enabled."},
		{Name: "wiki_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has the wiki enabled."},
		{Name: "snippets_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has snippets enabled."},
		{Name: "container_registry_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has the container registry enabled."},
		{Name: "creator_id", Type: proto.ColumnType_INT, Description: "The ID of the projects creator. - link to `gitlab_user.id`"},
		{Name: "created_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of when project was created."},
		{Name: "last_activity_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of when last activity happened on the project."},
		{Name: "marked_for_deletion_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of when project was marked for deletion.", Transform: transform.FromField("MarkedForDeletionAt").NullIfZero().Transform(isoTimeTransform)},
		{Name: "empty_repo", Type: proto.ColumnType_BOOL, Description: "Indicates if the repository of the project is empty."},
		{Name: "archived", Type: proto.ColumnType_BOOL, Description: "Indicates if the project is archived."},
		{Name: "avatar_url", Type: proto.ColumnType_STRING, Description: "The url for the projects avatar."},
		{Name: "forks_count", Type: proto.ColumnType_INT, Description: "The number of forks of the project.", Transform: transform.FromGo()},
		{Name: "star_count", Type: proto.ColumnType_INT, Description: "The number of stars given to the project.", Transform: transform.FromGo()},
		{Name: "lfs_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has large file system enabled.", Transform: transform.FromField("LFSEnabled")},
		{Name: "request_access_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has request access enabled."},
		{Name: "packages_enabled", Type: proto.ColumnType_BOOL, Description: "Indicates if the project has packages enabled."},
		{Name: "owner_id", Type: proto.ColumnType_INT, Description: "The projects owner ID. (null if owned by a group) - link to `gitlab_user.id`", Transform: transform.FromField("Owner.ID")},
		{Name: "owner_username", Type: proto.ColumnType_STRING, Description: "The projects owner username. (null if owned by a group) - link to `gitlab_user.username`", Transform: transform.FromField("Owner.Username")},
	}
}

func tableProject() *plugin.Table {
	return &plugin.Table{
		Name: "gitlab_project",
		Description: "Projects in the GitLab instance.",
		List: &plugin.ListConfig{
			Hydrate: listProjects,
		},
		Get: &plugin.GetConfig{
			KeyColumns: plugin.SingleColumn("id"),
			Hydrate: getProject,
		},
		Columns: projectColumns(),
	}
}

func listProjects(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	conn, err := connect(ctx, d)
	if err != nil {
		return nil, err
	}

	opt := &api.ListProjectsOptions{ListOptions: api.ListOptions{
		Page: 1,
		PerPage: 50,
	}}

	for {
		projects, resp, err := conn.Projects.ListProjects(opt)
		if err != nil {
			return nil, err
		}

		for _, project := range projects {
			d.StreamListItem(ctx, project)
		}

		if resp.NextPage == 0 {
			break
		}

		opt.Page = resp.NextPage
	}

	return nil, nil
}

func getProject(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	conn, err := connect(ctx, d)
	if err != nil {
		return nil, err
	}
	q := d.KeyColumnQuals
	id := int(q["id"].GetInt64Value())

	opt := &api.GetProjectOptions{}

	project, _, err := conn.Projects.GetProject(id, opt)
	if err != nil {
		return nil, err
	}
	return project, nil
}
