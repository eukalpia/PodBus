# PodBus documentation site

The public PodBus website and documentation are built with Next.js App Router and exported as static files for GitHub Pages.

## Local development

```bash
cd website
npm ci
npm run dev
```

The local server does not use the GitHub Pages `/PodBus` base path. Production builds enable it automatically when `GITHUB_ACTIONS=true`.

## Validation

```bash
npm run typecheck
GITHUB_ACTIONS=true npm run build
```

The static export is written to `website/out`. Code examples and capability names should be checked against the public package APIs whenever those APIs change.

## Content structure

Documentation pages are typed data rather than remote CMS entries:

- `content/getting-started.ts`
- `content/core-concepts.ts`
- `content/reliability.ts`
- `content/transports.ts`
- `content/integrations.ts`
- `content/operations.ts`
- `content/reference.ts`

`lib/docs.ts` assembles navigation, previous/next links, static routes, and the client-side search index. `components/doc-renderer.tsx` renders paragraphs, notes, steps, tables, and copyable code blocks consistently.

## Deployment

`.github/workflows/pages.yml` installs the locked dependencies, type-checks the project, builds the static export, validates representative routes, uploads `website/out`, and deploys it through the GitHub Pages environment.

The repository must use **Settings → Pages → Source: GitHub Actions** before the first deployment can publish the site.
