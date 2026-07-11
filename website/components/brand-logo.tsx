import { withBasePath } from '@/lib/site';

export function BrandLogo({
  className,
  decorative = false,
}: {
  className?: string;
  decorative?: boolean;
}) {
  const alt = decorative ? '' : 'PodBus';

  return (
    <span className={['brand-logo', className].filter(Boolean).join(' ')}>
      <img
        className="brand-logo-dark"
        src={withBasePath('/podbus-wordmark.svg')}
        alt={alt}
      />
      <img
        className="brand-logo-light"
        src={withBasePath('/podbus-wordmark-light.svg')}
        alt={alt}
      />
    </span>
  );
}
