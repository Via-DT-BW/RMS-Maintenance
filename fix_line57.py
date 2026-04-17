with open('C:/Users/josamorim/RMS-Maintenance/templates/projects.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()
lines[56] = '                        <button class="action-btn edit-btn me-2" title="Editar" data-id="{{ project.id }}" data-name="{{ project.name }}" data-desc="{{ project.description }}" data-resp="{{ project.responsible_list|tojson }}" onclick="openEditProject(this)">\n'
with open('C:/Users/josamorim/RMS-Maintenance/templates/projects.html', 'w', encoding='utf-8') as f:
    f.writelines(lines)
print('done')