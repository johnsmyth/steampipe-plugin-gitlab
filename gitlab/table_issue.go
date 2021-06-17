package gitlab

import (
	"context"
	"fmt"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/plugin"
	"github.com/turbot/steampipe-plugin-sdk/plugin/transform"
	api "github.com/xanzy/go-gitlab"
)

const GitlabCloudApiUrl = "https://gitlab.com/api/v4"

func issueColumns() []*plugin.Column {
	return []*plugin.Column{
		{Name: "id", Type: proto.ColumnType_INT, Description: "The ID of the Issue."},
		{Name: "title", Type: proto.ColumnType_STRING, Description: "The title of the Issue."},
		{Name: "description", Type: proto.ColumnType_STRING, Description: "The description of the Issue."},
		{Name: "state", Type: proto.ColumnType_STRING, Description: "The state of the Issue (opened, closed, etc)."},
		{Name: "project_id", Type: proto.ColumnType_INT, Description: "The ID of the project - link to `gitlab_project.id`."},
		{Name: "external_id", Type: proto.ColumnType_STRING, Description: "The external ID of the issue."},
		{Name: "author_id", Type: proto.ColumnType_INT, Description: "The ID of the author - link to `gitlab_user.id`.", Transform: transform.FromField("Author.ID")},
		{Name: "author", Type: proto.ColumnType_STRING, Description: "The username of the author - link to `gitlab_user.username`.", Transform: transform.FromField("Author.Username")},
		{Name: "created_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of issue creation."},
		{Name: "updated_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of last update to the issue."},
		{Name: "closed_at", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of when issue was closed. (null if not closed)."},
		{Name: "closed_by_id", Type: proto.ColumnType_INT, Description: "The ID of the user whom closed the issue - link to `gitlab_user.id`.", Transform: transform.FromField("ClosedBy.ID")},
		{Name: "closed_by", Type: proto.ColumnType_STRING, Description: "The username of the user whom closed the issue - link to `gitlab_user.username`.", Transform: transform.FromField("ClosedBy.Username")},
		{Name: "assignee_id", Type: proto.ColumnType_INT, Description: "The ID of the user assigned to the issue - link to `gitlab_user.id`.", Transform: transform.FromField("Assignee.ID")},
		{Name: "assignee", Type: proto.ColumnType_STRING, Description: "The username of the user assigned to the issue - link to `gitlab_user.username`", Transform: transform.FromField("Assignee.Username")},
		{Name: "assignees", Type: proto.ColumnType_JSON, Description: "An array of assigned usernames, for when more than one user is assigned.", Transform: transform.FromField("Assignees").Transform(parseAssignees)},
		{Name: "upvotes", Type: proto.ColumnType_INT, Description: "Count of up-votes received on the issue.", Transform: transform.FromGo()},
		{Name: "downvotes", Type: proto.ColumnType_INT, Description: "Count of down-votes received on the issue.", Transform: transform.FromGo()},
		{Name: "due_date", Type: proto.ColumnType_TIMESTAMP, Description: "Timestamp of due date for the issue to be completed by.", Transform: transform.FromField("DueDate").NullIfZero().Transform(isoTimeTransform)},
		{Name: "web_url", Type: proto.ColumnType_STRING, Description: "The url to access the issue.", Transform: transform.FromField("WebURL")},
		{Name: "confidential", Type: proto.ColumnType_BOOL, Description: "Indicates if the issue is marked as confidential."},
		{Name: "discussion_locked", Type: proto.ColumnType_BOOL, Description: "Indicates if the issue has the discussions locked against new input."},

		{Name: "search_string", Type: proto.ColumnType_STRING, Description: "Search string to limit results", Hydrate: searchStringFromQuals, Transform: transform.FromValue()},
	}
}

func tableIssue() *plugin.Table {
	return &plugin.Table{
		Name:        "gitlab_issue",
		Description: "All GitLab Issues",
		List: &plugin.ListConfig{
			Hydrate:            listIssues,
			OptionalKeyColumns: plugin.AnyColumn([]string{"assignee", "assignee_id", "author", "author_id", "confidential", "search_string", "project_id"}),
		},
		Columns: issueColumns(),
	}
}

func listIssues(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	q := d.OptionalKeyColumnQuals

	// if using gitlab cloud, at leas one of these must be set
	// or we'll get a 500 error because theres too much to search...
	if q["assignee"] == nil &&
		q["assignee_id"] == nil &&
		q["author_id"] == nil &&
		q["project_id"] == nil &&
		isGitlabCloud(d) {
		return nil, fmt.Errorf("When using this table with gitlab cloud, 'List' call requires an '=' qual for " +
			"one ore more of the following columns:  assignee, assignee_id, author_id, project_id")
	}

	if q["project_id"] != nil {
		return listProjectIssues(ctx, d, h)
	}

	return listAllIssues(ctx, d, h)

}

func listAllIssues(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	q := d.OptionalKeyColumnQuals

	conn, err := connect(ctx, d)
	if err != nil {
		return nil, err
	}

	defaultScope := "all"
	opt := &api.ListIssuesOptions{
		Scope: &defaultScope,
		ListOptions: api.ListOptions{
			Page:    1,
			PerPage: 50,
		},
	}
	opt = addOptionalIssueQuals(ctx, opt, q)

	for {
		issues, resp, err := conn.Issues.ListIssues(opt)
		if err != nil {
			return nil, err
		}

		for _, issue := range issues {
			d.StreamListItem(ctx, issue)
		}

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return nil, nil
}

func listProjectIssues(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	conn, err := connect(ctx, d)
	if err != nil {
		return nil, err
	}

	defaultScope := "all"
	opt := &api.ListProjectIssuesOptions{
		Scope: &defaultScope,
		ListOptions: api.ListOptions{
			Page:    1,
			PerPage: 50,
		},
	}
	q := d.OptionalKeyColumnQuals
	opt = addOptionalProjectIssueQuals(ctx, opt, q)

	project_id := 0
	if q["project_id"] == nil {
		return nil, nil
	} else {
		project_id = int(q["project_id"].GetInt64Value())
	}

	for {
		var issues []*api.Issue
		var resp *api.Response

		issues, resp, err = conn.Issues.ListProjectIssues(project_id, &api.ListProjectIssuesOptions{})
		if err != nil {
			return nil, err
		}

		for _, issue := range issues {
			d.StreamListItem(ctx, issue)
		}

		if resp.NextPage == 0 {
			break
		}
		opt.Page = resp.NextPage
	}

	return nil, nil
}

func addOptionalIssueQuals(ctx context.Context, opts *api.ListIssuesOptions, q map[string]*proto.QualValue) *api.ListIssuesOptions {
	logger := plugin.Logger(ctx)

	if q["assignee"] != nil {
		assignee := q["assignee"].GetStringValue()
		logger.Debug("addOptionalQuals", "assignee", assignee)
		opts.AssigneeUsername = &assignee
	}

	if q["assignee_id"] != nil {
		assignee_id := int(q["assignee_id"].GetInt64Value())
		logger.Debug("addOptionalQuals", "assignee_id", assignee_id)
		opts.AssigneeID = &assignee_id
	}

	// api docs suggest this can work, but not implemented in the go lib?
	// if q["author"] != nil {
	// 	author := q["author"].GetStringValue()
	// 	logger.Debug("addOptionalQuals", "author", author)
	// 	opts.AuthorUsername = &author
	// }

	if q["author_id"] != nil {
		author_id := int(q["author_id"].GetInt64Value())
		logger.Debug("addOptionalQuals", "author_id", author_id)
		opts.AuthorID = &author_id
	}

	if q["confidential"] != nil {
		confidential := q["confidential"].GetBoolValue()
		logger.Debug("addOptionalQuals", "confidential", confidential)
		opts.Confidential = &confidential
	}

	if q["search_string"] != nil {
		search_string := q["search_string"].GetStringValue()
		logger.Debug("addOptionalQuals", "assignee", search_string)
		opts.Search = &search_string
	}

	return opts
}

func addOptionalProjectIssueQuals(ctx context.Context, opts *api.ListProjectIssuesOptions, q map[string]*proto.QualValue) *api.ListProjectIssuesOptions {
	logger := plugin.Logger(ctx)

	if q["assignee"] != nil {
		assignee := q["assignee"].GetStringValue()
		logger.Debug("addOptionalQuals", "assignee", assignee)
		opts.AssigneeUsername = &assignee
	}

	if q["assignee_id"] != nil {
		assignee_id := int(q["assignee_id"].GetInt64Value())
		logger.Debug("addOptionalQuals", "assignee_id", assignee_id)
		opts.AssigneeID = &assignee_id
	}

	// api docs suggest this can work, but not implemented in the go lib?
	// if q["author"] != nil {
	// 	author := q["author"].GetStringValue()
	// 	logger.Debug("addOptionalQuals", "author", author)
	// 	opts.AuthorUsername = &author
	// }

	if q["author_id"] != nil {
		author_id := int(q["author_id"].GetInt64Value())
		logger.Debug("addOptionalQuals", "author_id", author_id)
		opts.AuthorID = &author_id
	}

	if q["confidential"] != nil {
		confidential := q["confidential"].GetBoolValue()
		logger.Debug("addOptionalQuals", "confidential", confidential)
		opts.Confidential = &confidential
	}

	if q["search_string"] != nil {
		search_string := q["search_string"].GetStringValue()
		logger.Debug("addOptionalQuals", "assignee", search_string)
		opts.Search = &search_string
	}

	return opts
}

// Transform Functions
func parseAssignees(ctx context.Context, input *transform.TransformData) (interface{}, error) {
	if input.Value == nil {
		return nil, nil
	}

	assignees := input.Value.([]*api.IssueAssignee)
	var output []string

	for _, assignee := range assignees {
		output = append(output, assignee.Username)
	}

	return output, nil
}

func searchStringFromQuals(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	quals := d.OptionalKeyColumnQuals
	q := quals["search_string"].GetStringValue()
	return q, nil
}
