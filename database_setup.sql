-- ============================================
-- RMS-Maintenance Database Setup
-- Pedidos de Manutenção (MTSE, MTQA, MTEX, MTREP)
-- ============================================

USE master;
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'RMS-Maintenance')
BEGIN
    CREATE DATABASE RMS-Maintenance;
END
GO

USE RMS-Maintenance;
GO

-- ============================================
-- Tabela principal: maintenance_requests
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'maintenance_requests')
BEGIN
    CREATE TABLE maintenance_requests (
        id INT IDENTITY(1,1) PRIMARY KEY,
        internal_code NVARCHAR(50) NOT NULL UNIQUE,
        type NVARCHAR(10) NOT NULL,  -- MTSE, MTQA, MTEX, MTREP
        
        title NVARCHAR(255) NOT NULL,
        line NVARCHAR(100) NULL,
        equipment NVARCHAR(100) NULL,
        description NVARCHAR(MAX) NULL,
        
        eight_d_number NVARCHAR(50) NULL,
        d3 BIT DEFAULT 0,
        d7 BIT DEFAULT 0,
        
        filename NVARCHAR(255) NULL,
        
        requester NVARCHAR(100) NOT NULL,
        requester_name NVARCHAR(255) NULL,
        requester_email NVARCHAR(255) NULL,
        
        status INT DEFAULT 0,
        approved BIT DEFAULT 0,
        responsible NVARCHAR(100) NULL,
        responsible_name NVARCHAR(255) NULL,
        expected_date NVARCHAR(100) NULL,
        rejection_reason NVARCHAR(MAX) NULL,
        
        cc_emails NVARCHAR(MAX) NULL,
        
        notes NVARCHAR(MAX) NULL,
        observations NVARCHAR(MAX) NULL,
        requester_response NVARCHAR(MAX) NULL,
        
        order_id NVARCHAR(100) NULL,
        completion_datetime DATETIME NULL,
        time_spent INT NULL,
        
        material_request_number NVARCHAR(100) NULL,
        closing_filename NVARCHAR(255) NULL,
        
        created_at DATETIME DEFAULT GETDATE(),
        updated_at DATETIME DEFAULT GETDATE(),
        
        is_deleted BIT DEFAULT 0
    );
    
    PRINT 'Tabela maintenance_requests criada.';
END
ELSE
BEGIN
    PRINT 'Tabela maintenance_requests já existe.';
END
GO

-- ============================================
-- Tabela: users
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'users')
BEGIN
    CREATE TABLE users (
        id INT PRIMARY KEY,
        username NVARCHAR(255) NOT NULL,
        password NVARCHAR(255) NOT NULL,
        email NVARCHAR(255),
        role INT DEFAULT 1,
        category NVARCHAR(255) NULL,
        name NVARCHAR(255) NULL,
        area NVARCHAR(255) NULL,
        turno NVARCHAR(255) NULL
    );
    
    PRINT 'Tabela users criada.';
END
ELSE
BEGIN
    PRINT 'Tabela users já existe.';
END
GO

-- ============================================
-- Índices
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_mr_internal_code' AND object_id = OBJECT_ID('maintenance_requests'))
    CREATE UNIQUE INDEX IX_mr_internal_code ON maintenance_requests(internal_code);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_mr_type' AND object_id = OBJECT_ID('maintenance_requests'))
    CREATE INDEX IX_mr_type ON maintenance_requests(type);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_mr_requester' AND object_id = OBJECT_ID('maintenance_requests'))
    CREATE INDEX IX_mr_requester ON maintenance_requests(requester);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_mr_status' AND object_id = OBJECT_ID('maintenance_requests'))
    CREATE INDEX IX_mr_status ON maintenance_requests(status);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_mr_requester_type' AND object_id = OBJECT_ID('maintenance_requests'))
    CREATE INDEX IX_mr_requester_type ON maintenance_requests(requester, type);
GO

-- ============================================
-- Tabela: tasks
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'tasks')
BEGIN
    CREATE TABLE tasks (
        id INT IDENTITY(1,1) PRIMARY KEY,
        category NVARCHAR(50) NULL,
        title NVARCHAR(255) NOT NULL,
        description NVARCHAR(MAX) NULL,
        responsible NVARCHAR(100) NULL,
        priority NVARCHAR(50) NULL,
        status NVARCHAR(50) DEFAULT 'To Do',
        start_date DATETIME NULL,
        end_date DATETIME NULL,
        week_number NVARCHAR(10) NULL,
        ticket_internal_code NVARCHAR(50) NULL,
        ticket_table NVARCHAR(50) NULL,
        created_at DATETIME DEFAULT GETDATE(),
        updated_at DATETIME DEFAULT GETDATE(),
        time_spent INT NULL,
        planned_start_date DATETIME NULL,
        planned_end_date DATETIME NULL,
        project_id INT NULL,
        comments NVARCHAR(MAX) NULL,
        estimated_hours INT NULL,
        created_by NVARCHAR(100) NULL,
        task_type NVARCHAR(50) NULL,
        is_principal_task BIT DEFAULT 0,
        principal_task_id INT NULL,
        github_issue_number INT NULL,
        github_issue_url NVARCHAR(500) NULL,
        is_deleted BIT DEFAULT 0
    );
    
    PRINT 'Tabela tasks criada.';
END
ELSE
BEGIN
    PRINT 'Tabela tasks já existe.';
END
GO

-- ============================================
-- Índices para tasks
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_tasks_ticket_code' AND object_id = OBJECT_ID('tasks'))
    CREATE INDEX IX_tasks_ticket_code ON tasks(ticket_internal_code);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_tasks_responsible' AND object_id = OBJECT_ID('tasks'))
    CREATE INDEX IX_tasks_responsible ON tasks(responsible);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_tasks_status' AND object_id = OBJECT_ID('tasks'))
    CREATE INDEX IX_tasks_status ON tasks(status);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_tasks_week' AND object_id = OBJECT_ID('tasks'))
    CREATE INDEX IX_tasks_week ON tasks(week_number);

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_tasks_principal' AND object_id = OBJECT_ID('tasks'))
    CREATE INDEX IX_tasks_principal ON tasks(principal_task_id);

GO

-- ============================================
-- Tabela: projects
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'projects')
BEGIN
    CREATE TABLE projects (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        description NVARCHAR(MAX) NULL,
        responsible NVARCHAR(MAX) NULL,
        created_by NVARCHAR(100) NULL,
        created_at DATETIME DEFAULT GETDATE(),
        updated_at DATETIME DEFAULT GETDATE(),
        is_deleted BIT DEFAULT 0
    );
    
    PRINT 'Tabela projects criada.';
END
ELSE
BEGIN
    PRINT 'Tabela projects já existe.';
END
GO

-- ============================================
-- Índices para projects
-- ============================================

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_projects_responsible' AND object_id = OBJECT_ID('projects'))
    CREATE INDEX IX_projects_responsible ON projects(responsible);

GO

-- ============================================
-- Verificar estrutura
-- ============================================

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'maintenance_requests'
ORDER BY ORDINAL_POSITION;
GO

PRINT 'Setup concluído!';
GO
