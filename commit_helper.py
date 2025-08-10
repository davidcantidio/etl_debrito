#!/usr/bin/env python3
"""
🎯 ETL Debrito - Commit Helper CLI
=================================

Interactive helper para criar commits padronizados seguindo o padrão TDD enhanced:
[EPIC-X] tdd-phase: conv-type: description [Task ID | Xmin]

Enhanced pattern com TDD phases para Projects v2 integration.

Uso:
    python commit_helper.py           # Modo interativo
    python commit_helper.py --quick   # Modo rápido (sem task info)
    python commit_helper.py --setup   # Instalar git hooks
"""

import argparse
import subprocess
import sys
import os
import json
import re
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime


class CommitHelper:
    """Helper interativo para commits padronizados."""
    
    def __init__(self):
        # TDD Phases (primary categorization)
        self.tdd_phases = {
            'analysis': '📋 Análise, planejamento e documentação',
            'red': '🔴 Escrevendo/ajustando testes que falham', 
            'green': '🟢 Implementando código para passar nos testes',
            'refactor': '🟨 Melhorando código sem quebrar testes'
        }
        
        # Conventional commit types (secondary categorization)
        self.commit_types = {
            'feat': 'Nova funcionalidade ou capability',
            'fix': 'Correção de bug ou problema',  
            'test': 'Adicionar/modificar testes',
            'refactor': 'Refatoração de código',
            'docs': 'Atualizar documentação, comentários',
            'perf': 'Melhoria de performance',
            'style': 'Formatação, linting, style fixes',
            'chore': 'Tarefas de manutenção, build, etc.',
            'milestone': 'Marco importante ou épico completo'
        }
        
        self.epics_cache = self.load_available_epics()
        
    def load_available_epics(self) -> Dict[str, str]:
        """Carrega épicos disponíveis dos arquivos JSON."""
        epics = {}
        
        for epic_file in Path(".").glob("epico_*.json"):
            try:
                with open(epic_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                epic_data = data.get('epic', {})
                epic_id = str(epic_data.get('id', ''))
                epic_name = epic_data.get('name', 'Unknown Epic')
                
                if epic_id:
                    epics[epic_id] = epic_name
                    
            except Exception as e:
                print(f"⚠️ Erro ao carregar {epic_file}: {e}")
        
        return epics
    
    def show_welcome(self):
        """Mostra banner de boas-vindas."""
        print("=" * 60)
        print("🎯 ETL DEBRITO - COMMIT HELPER")
        print("=" * 60)
        print("Cria commits padronizados para tracking automático")
        print()
    
    def select_epic(self) -> str:
        """Seleção interativa de épico."""
        if not self.epics_cache:
            print("⚠️ Nenhum épico encontrado nos arquivos JSON")
            return input("Digite o Epic ID manualmente: ")
        
        print("📋 Épicos Disponíveis:")
        print("-" * 40)
        
        epic_list = list(self.epics_cache.items())
        for i, (epic_id, epic_name) in enumerate(epic_list, 1):
            print(f"  {i:2}. Epic {epic_id}: {epic_name[:50]}{'...' if len(epic_name) > 50 else ''}")
        
        print(f"  {len(epic_list)+1:2}. Outro (digitar manualmente)")
        print()
        
        while True:
            try:
                choice = input("Selecione o épico (1-{}): ".format(len(epic_list)+1))
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(epic_list):
                    return epic_list[choice_num - 1][0]
                elif choice_num == len(epic_list) + 1:
                    return input("Digite o Epic ID: ")
                else:
                    print(f"❌ Opção inválida. Use 1-{len(epic_list)+1}")
            except ValueError:
                print("❌ Digite um número válido")
    
    def select_tdd_phase(self) -> str:
        """Seleção interativa da fase TDD."""
        print("\n🧪 Fase TDD:")
        print("-" * 40)
        
        phases_list = list(self.tdd_phases.items())
        for i, (phase_name, description) in enumerate(phases_list, 1):
            print(f"  {i}. {phase_name:<10} - {description}")
        
        print()
        
        while True:
            try:
                choice = input(f"Selecione a fase TDD (1-{len(phases_list)}): ")
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(phases_list):
                    return phases_list[choice_num - 1][0]
                else:
                    print(f"❌ Opção inválida. Use 1-{len(phases_list)}")
            except ValueError:
                print("❌ Digite um número válido")
    
    def select_commit_type(self) -> str:
        """Seleção interativa do tipo de commit."""
        print("\n🏷️ Tipos de Commit:")
        print("-" * 40)
        
        types_list = list(self.commit_types.items())
        for i, (type_name, description) in enumerate(types_list, 1):
            print(f"  {i}. {type_name:<12} - {description}")
        
        print()
        
        while True:
            try:
                choice = input(f"Selecione o tipo (1-{len(types_list)}): ")
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(types_list):
                    return types_list[choice_num - 1][0]
                else:
                    print(f"❌ Opção inválida. Use 1-{len(types_list)}")
            except ValueError:
                print("❌ Digite um número válido")
    
    def get_commit_description(self) -> str:
        """Obter descrição do commit."""
        print("\n📝 Descrição do Commit:")
        print("-" * 40)
        print("Descreva brevemente o que foi feito (ex: 'implement warning interceptor core')")
        
        while True:
            description = input("Descrição: ").strip()
            if description:
                return description
            print("❌ Descrição não pode estar vazia")
    
    def get_extended_body(self) -> Optional[str]:
        """Obter corpo estendido opcional."""
        print("\n📄 Corpo Estendido (opcional):")
        print("-" * 40)
        print("Adicione detalhes sobre a implementação, decisões técnicas, etc.")
        print("Pressione ENTER para pular, ou digite '.' numa linha para finalizar")
        
        body_lines = []
        while True:
            line = input("  > ").strip()
            if line == "" and len(body_lines) == 0:
                return None
            elif line == ".":
                break
            else:
                body_lines.append(line)
        
        return "\n".join(body_lines) if body_lines else None
    
    def get_task_info(self) -> Optional[Dict[str, str]]:
        """Obter informações da task (opcional mas recomendado)."""
        print("\n🎯 Informações da Task:")
        print("-" * 40)
        print("Para tracking de progresso e métricas de accuracy")
        
        add_task = input("Adicionar info da task? (Y/n): ").lower() != 'n'
        if not add_task:
            return None
        
        task_info = {}
        
        # Task ID
        print("\nExemplos de Task ID: 3.1a, 3.1b.2, 8.2c.1, etc.")
        task_info['id'] = input("Task ID: ").strip()
        
        # Time spent
        while True:
            try:
                time_str = input("Tempo gasto (minutos): ").strip()
                task_info['time'] = int(time_str)
                break
            except ValueError:
                print("❌ Digite um número válido para minutos")
        
        # Task info não precisa mais de TDD status (agora é fase separada)
        
        return task_info
    
    def build_commit_message(self, epic_id: str, tdd_phase: str, commit_type: str, 
                           description: str, body: Optional[str] = None,
                           task_info: Optional[Dict] = None) -> str:
        """Constrói a mensagem do commit completa usando padrão TDD enhanced."""
        
        # Enhanced header: [EPIC-X] tdd-phase: conv-type: description [Task X.Y | Zmin]
        header = f"[EPIC-{epic_id}] {tdd_phase}: {commit_type}: {description}"
        
        # Add task info directly in header if provided
        if task_info:
            header += f" [Task {task_info['id']} | {task_info['time']}min]"
        
        parts = [header]
        
        # Body (se fornecido)
        if body:
            parts.extend(["", body])
        
        return "\n".join(parts)
    
    def preview_commit(self, message: str) -> bool:
        """Mostra preview do commit e confirma."""
        print("\n" + "="*60)
        print("📋 PREVIEW DO COMMIT:")
        print("="*60)
        print(message)
        print("="*60)
        
        confirm = input("\n✅ Confirma este commit? (Y/n): ").lower() != 'n'
        return confirm
    
    def create_commit(self, message: str) -> bool:
        """Cria o commit usando git."""
        try:
            # Verificar se há mudanças para commit
            result = subprocess.run(['git', 'diff', '--cached', '--quiet'], 
                                  capture_output=True)
            
            if result.returncode == 0:
                # Nada staged, tentar add automático
                print("ℹ️ Nenhum arquivo staged. Mostrando arquivos modificados...")
                
                status_result = subprocess.run(['git', 'status', '--porcelain'], 
                                             capture_output=True, text=True)
                
                if status_result.returncode == 0 and status_result.stdout.strip():
                    print("\n📁 Arquivos modificados:")
                    for line in status_result.stdout.strip().split('\n'):
                        print(f"  {line}")
                    
                    add_all = input("\nAdicionar todos os arquivos modificados? (Y/n): ").lower() != 'n'
                    if add_all:
                        subprocess.run(['git', 'add', '.'], check=True)
                    else:
                        print("❌ Commit cancelado. Use 'git add' para preparar os arquivos")
                        return False
                else:
                    print("❌ Nenhuma mudança encontrada para commit")
                    return False
            
            # Fazer o commit
            subprocess.run(['git', 'commit', '-m', message], check=True)
            print("✅ Commit criado com sucesso!")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Erro ao criar commit: {e}")
            return False
    
    def interactive_commit(self):
        """Fluxo interativo completo para criar commit."""
        self.show_welcome()
        
        try:
            # Coleta de informações
            epic_id = self.select_epic()
            tdd_phase = self.select_tdd_phase()
            commit_type = self.select_commit_type()
            description = self.get_commit_description()
            body = self.get_extended_body()
            task_info = self.get_task_info()
            
            # Construir mensagem
            message = self.build_commit_message(epic_id, tdd_phase, commit_type, description, body, task_info)
            
            # Preview e confirmação
            if self.preview_commit(message):
                if self.create_commit(message):
                    # Mostrar próximos passos
                    print("\n🎉 Próximos Passos:")
                    print("  • Execute 'python gantt_tracker.py' para ver o progresso")
                    print("  • Faça push quando estiver pronto")
                    print("  • Continue seguindo o padrão para todas as mudanças")
                else:
                    return False
            else:
                print("❌ Commit cancelado")
                return False
                
        except KeyboardInterrupt:
            print("\n\n❌ Operação cancelada pelo usuário")
            return False
        
        return True
    
    def quick_commit(self, epic_id: Optional[str] = None, 
                    tdd_phase: Optional[str] = None,
                    commit_type: Optional[str] = None,
                    description: Optional[str] = None):
        """Modo rápido para commits simples sem task info."""
        
        if not epic_id:
            epic_id = self.select_epic()
        
        if not tdd_phase:
            tdd_phase = self.select_tdd_phase()
        
        if not commit_type:
            commit_type = self.select_commit_type()
        
        if not description:
            description = self.get_commit_description()
        
        message = self.build_commit_message(epic_id, tdd_phase, commit_type, description)
        
        if self.preview_commit(message):
            return self.create_commit(message)
        
        return False
    
    def install_git_hooks(self):
        """Instala git hooks para validação automática."""
        hooks_dir = Path(".git/hooks")
        
        if not hooks_dir.exists():
            print("❌ Diretório .git/hooks não encontrado. Certifique-se de estar em um repo git")
            return False
        
        commit_msg_hook = hooks_dir / "commit-msg"
        
        hook_content = '''#!/bin/sh
# ETL Debrito - Commit Message Validation Hook
# Validates commit messages follow enhanced TDD pattern

# Enhanced TDD pattern: [EPIC-X] tdd-phase: conv-type: description [Task X.Y | Zmin]
enhanced_regex='^\\[EPIC-[0-9]+\\.?[0-9]*\\] (analysis|red|green|refactor): (feat|fix|test|refactor|docs|perf|style|chore|milestone): .+'

# Legacy pattern for backward compatibility
legacy_regex='^\\[EPIC-[0-9]+\\.?[0-9]*\\] (feat|fix|test|refactor|docs|milestone|wip): .+'

if grep -qE "$enhanced_regex" "$1" || grep -qE "$legacy_regex" "$1"; then
    # Success - commit message is valid
    exit 0
fi

echo ""
echo "❌ COMMIT MESSAGE FORMAT INVALID!"
echo ""
echo "Enhanced TDD format (recommended):"
echo "  [EPIC-X] tdd-phase: conv-type: description [Task X.Y | Zmin]"
echo ""
echo "TDD phases: analysis, red, green, refactor"
echo "Conv types: feat, fix, test, refactor, docs, perf, style, chore, milestone"
echo ""
echo "Examples:"
echo "  [EPIC-3] red: test: add warning interceptor tests [Task 3.2 | 15min]"
echo "  [EPIC-0] green: feat: implement config loader [Task 0.1 | 20min]"
echo "  [EPIC-8] refactor: refactor: optimize timer accuracy"
echo ""
echo "Legacy format (still supported):"
echo "  [EPIC-X] type: description"
echo ""
echo "Use 'python commit_helper.py' for guided commit creation"
echo ""
exit 1
'''
        
        try:
            commit_msg_hook.write_text(hook_content, encoding='utf-8')
            commit_msg_hook.chmod(0o755)  # Make executable
            
            print("✅ Git hook instalado com sucesso!")
            print(f"   📁 {commit_msg_hook}")
            print("   🛡️ Commits inválidos serão rejeitados automaticamente")
            return True
            
        except Exception as e:
            print(f"❌ Erro ao instalar git hook: {e}")
            return False
    
    def validate_message(self, message: str) -> bool:
        """Valida se uma mensagem segue o padrão (enhanced ou legacy)."""
        lines = message.strip().split('\n')
        if not lines:
            return False
        
        header = lines[0]
        
        # Enhanced TDD pattern
        enhanced_pattern = r'^\[EPIC-\d+\.?\d*\] (analysis|red|green|refactor): (feat|fix|test|refactor|docs|perf|style|chore|milestone): .+'
        
        # Legacy pattern
        legacy_pattern = r'^\[EPIC-\d+\.?\d*\] (feat|fix|test|refactor|docs|milestone|wip): .+'
        
        return bool(re.match(enhanced_pattern, header) or re.match(legacy_pattern, header))


def main():
    """CLI principal."""
    parser = argparse.ArgumentParser(
        description="🎯 ETL Debrito - Commit Helper CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python commit_helper.py                    # Modo interativo completo
  python commit_helper.py --quick            # Modo rápido (sem task info)
  python commit_helper.py --setup            # Instalar git hooks
  
Enhanced TDD Pattern (recomendado):
  [EPIC-X] tdd-phase: conv-type: description [Task X.Y | Zmin]
  
  TDD Phases: analysis, red, green, refactor
  Conv Types: feat, fix, test, refactor, docs, perf, style, chore, milestone
  
Exemplos:
  [EPIC-3] red: test: add warning interceptor tests [Task 3.2 | 15min]
  [EPIC-0] green: feat: implement config loader [Task 0.1 | 20min]
  [EPIC-8] refactor: refactor: optimize timer accuracy
  
Legacy Pattern (ainda suportado):
  [EPIC-X] type: description
        """
    )
    
    parser.add_argument(
        "--quick", "-q",
        action="store_true",
        help="Modo rápido sem informações de task"
    )
    
    parser.add_argument(
        "--setup", "-s",
        action="store_true",
        help="Instalar git hooks para validação"
    )
    
    parser.add_argument(
        "--epic", "-e",
        help="Epic ID para modo rápido"
    )
    
    parser.add_argument(
        "--type", "-t",
        choices=['feat', 'fix', 'test', 'refactor', 'docs', 'milestone', 'wip'],
        help="Tipo do commit para modo rápido"
    )
    
    parser.add_argument(
        "--description", "-d",
        help="Descrição do commit para modo rápido"
    )
    
    args = parser.parse_args()
    
    helper = CommitHelper()
    
    try:
        if args.setup:
            return 0 if helper.install_git_hooks() else 1
        elif args.quick:
            return 0 if helper.quick_commit(args.epic, args.type, args.description) else 1
        else:
            return 0 if helper.interactive_commit() else 1
            
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
        return 1


if __name__ == "__main__":
    exit(main())