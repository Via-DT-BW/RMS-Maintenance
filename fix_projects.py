import re

with open('C:/Users/josamorim/RMS-Maintenance/templates/projects.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the problematic edit button line - simpler approach
old_line = '''<button class="action-btn edit-btn me-2" title="Editar" onclick="editProject({{ project.id }}, '{{ project.name|replace(\"'\", \"\\\\'\") }}', '{{ project.description|replace(\"'\", \"\\\\'\") }}', {{ project.responsible_list }})">'''

new_line = '''<button class="action-btn edit-btn me-2" title="Editar" data-id="{{ project.id }}" data-name="{{ project.name }}" data-desc="{{ project.description }}" data-resp="{{ project.responsible_list }}" onclick="openEditProject(this)">'''

content = content.replace(old_line, new_line)

with open('C:/Users/josamorim/RMS-Maintenance/templates/projects.html', 'w', encoding='utf-8') as f:
    f.write(content)

print('Done')