export type NoteTone = 'info' | 'warning' | 'success' | 'danger';

export type DocBlock =
  | {
      type: 'paragraph';
      text: string;
    }
  | {
      type: 'bullets';
      items: string[];
    }
  | {
      type: 'steps';
      items: Array<{ title: string; description: string }>;
    }
  | {
      type: 'code';
      language: string;
      code: string;
      filename?: string;
      caption?: string;
    }
  | {
      type: 'note';
      tone: NoteTone;
      title: string;
      text: string;
    }
  | {
      type: 'table';
      headers: string[];
      rows: string[][];
    };

export interface DocSection {
  id: string;
  title: string;
  blocks: DocBlock[];
}

export interface DocPage {
  slug: string;
  title: string;
  description: string;
  category: string;
  order: number;
  badge?: string;
  sections: DocSection[];
}

export interface DocCategory {
  title: string;
  order: number;
  pages: DocPage[];
}
