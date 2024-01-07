import { CommonModule } from '@angular/common';
import { ChangeDetectionStrategy, Component, Input } from '@angular/core';
import { Todo } from '../services/todos.service';

@Component({
  selector: 'query-prefetch-page',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './prefetch-page.component.html',
  styles: [],
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class PrefetchPageComponent {
  @Input() todos: Todo[] = [];
}
